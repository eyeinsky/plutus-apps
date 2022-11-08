{-

Indexer to keep track of the UTxO set.

This is unlike the simlarily named Marconi.Indexers.Utxo, where

-}

module Marconi.Indexers.UTxO where

import Data.Function ((&))
import Data.Set qualified as Set

import Cardano.Api qualified as C
import Cardano.Streaming
import Marconi.Streaming.ChainSync
import Streaming.Prelude qualified as S

type UTxO = C.TxIn

data Event = Event
  { used    :: ()
  , created :: ()
  }

toEvent
  :: S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO r
  -> S.Stream (S.Of Event) IO r
toEvent = S.map f
  where
    f (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) txs) _) = let
      usedUtxos = undefined
      createdUtxos = undefined
      _ = map txUtxos txs
      in undefined

    txUtxos :: forall era . C.Tx era -> ()
    txUtxos tx = case tx of
      C.Tx (C.TxBody (tbc :: C.TxBodyContent C.ViewTx era)) _ -> let
        consumed = undefined
        produced = undefined
        in undefined

indexer
  :: FilePath -> C.NetworkId -> C.ChainPoint
  -> S.Stream (S.Of Event) IO r
indexer socketPath networkId point = chainSyncEventSource socketPath networkId point
  & rollbackRingBuffer 2106
  & toEvent
