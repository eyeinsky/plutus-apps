{-# LANGUAGE LambdaCase    #-}
{-# LANGUAGE RankNTypes    #-}
{-# LANGUAGE TypeOperators #-}
module Marconi.Streaming.ChainSync where

import Control.Concurrent qualified as IO
import Control.Monad (void)
import Control.Monad.Trans.Class (lift)
import Data.Vector qualified as V
import Data.Vector.Generic qualified as VG
import Data.Vector.Generic.Mutable qualified as VGM
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
rollbackRingBuffer :: Int -> ChainSyncEvent a :-> (a, C.ChainTip)
rollbackRingBuffer bufferSize source = let
  split = S.splitAt bufferSize source
  in do

  -- fill the buffer, don't yield anything
  (vector :: Vec a) S.:> (rest :: S.Stream (S.Of a) IO r) <- lift $
    S.foldM
      (\(i, v) a -> case a of
          RollForward a' ct -> do
            VGM.unsafeWrite v i a
            pure (i + 1, v)
          RollBackward cp ct -> do
            undefined
      )
      (do vector <- VGM.new bufferSize
          pure (0, vector))
      (\(_, v) -> pure v)
      split

  -- fill & yield from the buffer
  -- let zippedWithIndex = S.zip (S.cycle (S.each [0 .. (bufferSize - 1)])) rest
  -- flip S.mapM zippedWithIndex $ \(i, a) -> do
  --   VGM.exchange vector i a
  undefined
