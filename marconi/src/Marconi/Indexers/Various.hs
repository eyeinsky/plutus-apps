{-# LANGUAGE LambdaCase #-}

module Marconi.Indexers.Various where

import Control.Monad.Trans.Class (lift)
import Data.Function ((&))

import Cardano.Api qualified as C
import Cardano.Streaming (ChainSyncEvent (RollBackward), chainSyncEventSource)
import Streaming.Prelude qualified as S


countRollbacks :: FilePath -> C.NetworkId -> C.ChainPoint -> S.Stream (S.Of ()) IO r
countRollbacks socketPath networkId point = chainSyncEventSource socketPath networkId point
  & countLoop 0
  where
    countLoop
      :: Integer
      -> S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
      -> S.Stream (S.Of ()) IO r
    countLoop n source = lift (S.next source) >>= \case
      Left r -> pure r
      Right (e, source') -> countLoop n' source'
        where n' = case e of
                RollBackward{} -> n + 1
                _              -> n
