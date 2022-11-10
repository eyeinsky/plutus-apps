{-# LANGUAGE LambdaCase     #-}
{-# LANGUAGE NamedFieldPuns #-}

module Marconi.Indexers.UTxO where

import Control.Monad.Trans.Class (lift)
import Data.Function ((&))
import Data.Set qualified as Set

import Cardano.Api qualified as C
import Cardano.Streaming (chainSyncEventSource)
import Marconi.Streaming.ChainSync (rollbackRingBuffer)
import Streaming.Prelude qualified as S

type UTxO = Set.Set C.TxIn

type Event = (UTxO, UTxO)

-- | Convert a stream of blocks into a stream of (spent, created) pairs
-- of UTxO
toEvent
  :: S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO r
  -> S.Stream (S.Of Event) IO r
toEvent source = S.for source blockToUtxos
  where
    -- Create stream of (spent, created) UTxO by transaction
    blockToUtxos :: C.BlockInMode C.CardanoMode -> S.Stream (S.Of (UTxO, UTxO)) IO ()
    blockToUtxos (C.BlockInMode (C.Block (C.BlockHeader _slotNo _ _) txs) _) = let
      spentCreatedByTx :: [(UTxO, UTxO)]
      spentCreatedByTx = map txToSpentCreated txs
      in S.each spentCreatedByTx

    -- Extract a set of (spent, created) UTxO from a transaction
    txToSpentCreated :: forall era . C.Tx era -> (UTxO, UTxO)
    txToSpentCreated tx = case tx of
      C.Tx (txb@(C.TxBody (txbc :: C.TxBodyContent C.ViewTx era))) _ -> let
        txOuts = C.txOuts txbc
        txOutsLength = toEnum $ length txOuts
        txId = C.getTxId txb
        mkTxIn word = C.TxIn txId (C.TxIx word)

        -- UTxO spent by transaction
        spentRegular = Set.fromList $ map fst $ C.txIns txbc :: UTxO
        spentCollateral = case C.txInsCollateral txbc of
          C.TxInsCollateral _ txIns -> Set.fromList txIns
          _                         -> Set.empty
        spentReference = case C.txInsReference txbc of
          C.TxInsReference _ txIns -> Set.fromList txIns
          _                        -> Set.empty

        -- UTxO created by transaction
        createdRegular :: UTxO
        createdRegular = Set.fromList $ map mkTxIn [0 .. (txOutsLength - 1)]
        createdFromReturnCollateral :: UTxO
        createdFromReturnCollateral = case C.txReturnCollateral txbc of
          C.TxReturnCollateral _ _txOuts -> Set.singleton $ C.TxIn txId (C.TxIx txOutsLength)
          _                              -> Set.empty

        -- Return a pair of (spent, created) UTxO by the transaction
        in ( spentRegular <> spentCollateral <> spentReference
           , createdRegular <> createdFromReturnCollateral)

-- | Fold a stream of (spent, created) UTxO to the set of current UTxO.
toUtxoAtBlock :: S.Stream (S.Of (UTxO, UTxO)) IO r -> S.Stream (S.Of UTxO) IO r
toUtxoAtBlock = let
  loop utxo source = lift (S.next source) >>= \case
    Left r -> pure r
    Right ((spent, created), source') -> let
      utxo' = (utxo `Set.difference` spent) `Set.union` created
      in S.yield utxo' >> loop utxo' source'
  in loop Set.empty

indexer
  :: FilePath -> C.NetworkId -> C.ChainPoint
  -> S.Stream (S.Of UTxO) IO r
indexer socketPath networkId point = chainSyncEventSource socketPath networkId point
  & rollbackRingBuffer 2160
  & toEvent
  & toUtxoAtBlock
