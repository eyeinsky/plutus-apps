module Marconi.Indexers.UTxO where

import Data.Function ((&))
import Data.Set qualified as Set

import Cardano.Api qualified as C
import Cardano.Streaming (chainSyncEventSource)
import Marconi.Streaming.ChainSync (rollbackRingBuffer)
import Streaming.Prelude qualified as S

type UTxO = Set.Set C.TxIn

type Event = (UTxO, UTxO)

-- | Convert a stream of blocks into a stream of (used, created) pairs
-- of UTxOs
toEvent
  :: S.Stream (S.Of (C.BlockInMode C.CardanoMode)) IO r
  -> S.Stream (S.Of Event) IO r
toEvent source = S.for source blockToUtxos
  where
    -- Create stream of (used, created) UTxOs by transaction
    -- blockToUtxos :: C.BlockInMode C.CardanoMode -> S.Stream (S.Of (UTxO, UTxO)) IO ()
    blockToUtxos (C.BlockInMode (C.Block (C.BlockHeader _slotNo _ _) txs) _) = let
      usedCreatedByTx :: [(UTxO, UTxO)]
      usedCreatedByTx = map txToUsedCreated txs
      in S.each usedCreatedByTx

    -- Extract a set of (used, created) UTxOs from a transaction
    txToUsedCreated :: forall era . C.Tx era -> (UTxO, UTxO)
    txToUsedCreated tx = case tx of
      C.Tx (txBody@(C.TxBody (tbc :: C.TxBodyContent C.ViewTx era))) _ -> let

        -- UTxOs used by the transaction
        usedRegular = Set.fromList $ map fst $ C.txIns tbc :: UTxO
        usedFromCollateral = case C.txInsCollateral tbc of
          C.TxInsCollateral _ txIns -> Set.fromList txIns
          _                         -> Set.empty
        usedFromReference = case C.txInsReference tbc of
          C.TxInsReference _ txIns -> Set.fromList txIns
          _                        -> Set.empty

        -- UTxOs created by the transaction
        txOutToRef' = txOutToRef txBody :: C.TxOut ctx era -> C.TxIn
        createdRegular :: UTxO
        createdRegular = txOutsToRefs tx
        createdFromReturnCollateral :: UTxO
        createdFromReturnCollateral = case C.txReturnCollateral tbc of
          C.TxReturnCollateral _ txOuts -> Set.singleton $ txOutToRef' txOuts
          _                             -> Set.empty

        -- Return a pair of (used, created) UTxOs by the transaction
        in ( usedRegular <> usedFromCollateral <> usedFromReference
           , createdRegular <> createdFromReturnCollateral)

    txOutToRef :: C.TxBody era -> C.TxOut ctx era -> C.TxIn
    txOutToRef txBody _ = C.TxIn (C.getTxId txBody) undefined

    txOutsToRefs :: C.Tx era -> UTxO
    txOutsToRefs (C.Tx (txb@(C.TxBody txbc)) _) = Set.fromList $ map (C.TxIn $ C.getTxId txb) $ map fst $ zip [C.TxIx 0 .. ] $ C.txOuts txbc



indexer
  :: FilePath -> C.NetworkId -> C.ChainPoint
  -> S.Stream (S.Of Event) IO r
indexer socketPath networkId point = chainSyncEventSource socketPath networkId point
  & rollbackRingBuffer 2160
  & toEvent
