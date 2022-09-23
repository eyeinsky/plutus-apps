{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE RankNTypes       #-}
{-# LANGUAGE TypeOperators    #-}
module Marconi.Streaming.ChainSync where

import Control.Concurrent qualified as IO
import Control.Monad (void)
import Control.Monad.Primitive (PrimState)
import Control.Monad.Trans.Class (lift)
import Data.Maybe (fromJust)
import Data.Vector qualified as V
import Data.Vector.Generic qualified as VG
import Data.Vector.Generic.Mutable qualified as VGM
import Data.Vector.Mutable qualified as VM
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Marconi.Streaming.Util
import Plutus.Streaming (ChainSyncEvent (RollBackward, RollForward), withChainSyncEventStream)

chainEventSource
  :: FilePath -> C.NetworkId -> C.ChainPoint
  -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
chainEventSource socket networkId chainPoint = do
  m <- lift IO.newEmptyMVar
  lift $ void $ IO.forkIO $ withChainSyncEventStream socket networkId chainPoint $ S.mapM_ (IO.putMVar m)
  S.repeatM $ IO.takeMVar m

-- | Ignores rollbacks, passes on only RollForward events as
-- tuple (e, ChainTip)
ignoreRollbacks :: ChainSyncEvent a :-> (a, C.ChainTip)
ignoreRollbacks = S.mapMaybe $ \case
  RollForward e chainTip -> Just (e, chainTip)
  _                      -> Nothing

-- | Ring buffer that handles rollbacks, throws if point of rollback
-- is not found.
rollbackRingBuffer
  :: forall a r
   . (VG.Vector Vec (a, C.ChainTip))
  => Int
  -> S.Stream (S.Of (ChainSyncEvent a)) IO r
  -> S.Stream (S.Of a) IO r
rollbackRingBuffer bufferSize source = fill 0 0 =<< lift (VGM.new bufferSize)
  where
    -- Fill phase, don't yield anything. Consume ChainSyncEvents and
    -- roll back to an earlier location in the vector in case of
    -- rollback.
    fill :: Int -> Int -> Vec (a, C.ChainTip) -> S.Stream (S.Of a) IO r
    fill i j vector = lift (S.next source) >>= \case
      Left r -> pure r
      Right (chainSyncEvent, s) -> case chainSyncEvent of
        RollForward a ct -> do
          lift $ VGM.unsafeWrite vector i (a, ct)
          let i' = i + 1
          if i' == bufferSize
            then fillYield i vector
            else fill i' (j + 1) vector
        RollBackward _cp ct -> rewind ct i j vector

    -- Fill & yield phase. Buffer is full in the beginning, but will
    -- need to be refilled when a rollback occurs.
    fillYield :: Int -> Vec (a, C.ChainTip) -> S.Stream (S.Of a) IO r
    fillYield i vector = lift (S.next source) >>= \case
      Left r -> pure r
      Right (chainSyncEvent, s) -> case chainSyncEvent of
        RollForward a ct -> do
          (a', _) <- lift $ VGM.exchange vector i (a, ct)
          S.yield a'
          fillYield (i `rem` bufferSize) vector
        RollBackward _cp ct -> rewind ct i bufferSize vector

    rewind :: C.ChainTip -> Int -> Int -> Vec (a, C.ChainTip) -> S.Stream (S.Of a) IO r
    rewind ct i j vector = let
      (i', j') = calcRewind ((ct ==) . snd) i j vector
      in fill i' j' vector

calcRewind
  :: (VG.Vector Vec a)
  => (a -> Bool) -> Int -> Int -> Vec a -> (Int, Int)
calcRewind ct i j vector = let
  -- Find index of the item that we rewind to
  Just i' = VG.findIndex ((ct ==) . snd) vector :: Maybe Int
  -- Calculate the number of elements that weren't discarded by the rollback
  j' = j - (if i > i' then i - i' else i + (i' - i))
  in (i', j')

testCalcRewindManual :: IO ()
testCalcRewindManual = do
  v :: Vec Int <- VGM.generate 10 id
--  VM.forM_ v $ (undefined :: _)

  return ()

{-

RollBackward cp _ct -> do
  events <- Ix.getEvents (index ^. Ix.storage)
    -- -> getInterval
    -- -> vector slice
    -- -> to list via foldr' (:)
  loop $
    fromMaybe index $ do
      slot   <- chainPointToSlotNo cp
      offset <- findIndex  (\u -> ScriptTx.slotNo u < slot) events
        -- ^ find offset in list (Maybe Int)
      Ix.rewind offset index
        -- adjust i and number of events (j)

Improve:
- don't construct that list, find index directly inside vector instead

-}
