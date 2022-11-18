{-# LANGUAGE LambdaCase #-}

module Marconi.Indexers.Various where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Data.Function ((&))
import Data.Maybe

import Cardano.Api qualified as C
import Cardano.Streaming (ChainSyncEvent (RollBackward), chainSyncEventSource)
import Streaming.Prelude qualified as S


rollbackCounter :: FilePath -> C.NetworkId -> C.ChainPoint -> S.Stream (S.Of ()) IO r
rollbackCounter socketPath networkId point = chainSyncEventSource socketPath networkId point
  & countLoop 0 Nothing
  where
    countLoop
      :: Integer
      -> Maybe C.SlotNo
      -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
      -> S.Stream (S.Of ()) IO r
    countLoop n maybeSlotNo source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> do
        case e of
          RollBackward{} -> printSlot maybeSlotNo n >> countLoop (n + 1) undefined source'
          _              -> countLoop n undefined source'


    printSlot maybeSlotNo count = lift $ print (count, maybeSlotNo)

getSlotNo :: C.BlockInMode mode -> C.SlotNo
getSlotNo (C.BlockInMode (C.Block (C.BlockHeader slotNo' _ _) _) _) = slotNo'
