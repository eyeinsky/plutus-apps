{-# LANGUAGE GADTs           #-}
{-# LANGUAGE NamedFieldPuns  #-}
{-# LANGUAGE PackageImports  #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TupleSections   #-}

module Marconi.Indexers where

import Control.Concurrent (forkIO)
import Control.Concurrent.QSemN (QSemN, newQSemN, signalQSemN, waitQSemN)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan (TChan, dupTChan, newBroadcastTChanIO, readTChan, writeTChan)
import Control.Exception (bracket_)
import Control.Lens.Combinators (imap)
import Control.Lens.Operators ((&), (^.))
import Control.Monad (void)
import Data.Foldable (foldl')
import Data.List (findIndex)
import Data.Maybe (catMaybes, fromMaybe, mapMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Streaming.Prelude qualified as S

import Cardano.Api qualified as C
import Cardano.Api.Byron qualified as Byron
import "cardano-api" Cardano.Api.Shelley qualified as Shelley
import Cardano.Ledger.Alonzo.TxWitness qualified as Alonzo
import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))
import Cardano.Api (Block (Block), BlockHeader (BlockHeader), BlockInMode (BlockInMode), CardanoMode, SlotNo,
                    chainPointToSlotNo)
import Marconi.Indexers.Datum (DatumIndex)
import Marconi.Indexers.Datum qualified as Datum
import Marconi.Indexers.Utxo (UtxoIndex, UtxoUpdate (UtxoUpdate, _inputs, _outputs, _slotNo))
import Marconi.Indexers.Utxo qualified as Utxo
import Marconi.Types (TargetAddresses, TxOut, TxOutRef, pattern CurrentEra, txOutRef)

import RewindableIndex.Index.VSplit qualified as Ix


{- | The way we synchronise channel consumption is by waiting on a QSemN for each
     of the spawn indexers to finish processing the current event.

     The channel is used to transmit the next event to the listening indexers. Note
     that even if the channel is unbound it will actually only ever hold one event
     because it will be blocked until the processing of the event finishes on all
     indexers.

     The indexer count is where we save the number of running indexers so we know for
     how many we are waiting.
-}
data Coordinator = Coordinator
  { _channel      :: TChan (ChainSyncEvent (BlockInMode CardanoMode))
  , _barrier      :: QSemN
  , _indexerCount :: Int
  }

initialCoordinator :: Int -> IO Coordinator
initialCoordinator indexerCount =
  Coordinator <$> newBroadcastTChanIO
              <*> newQSemN 0
              <*> pure indexerCount

type Worker = Coordinator -> TChan (ChainSyncEvent (BlockInMode CardanoMode)) -> FilePath -> IO ()

utxoWorker :: Maybe TargetAddresses -> Worker
utxoWorker maybeTargetAddresses Coordinator{_barrier} ch path =
    Utxo.open path (Utxo.Depth 2160) >>= innerLoop
  where
    innerLoop :: UtxoIndex -> IO ()
    innerLoop index = do
      signalQSemN _barrier 1
      event <- atomically $ readTChan ch
      case event of
        RollForward (BlockInMode (Block (BlockHeader slotNo _ _) txs) _) _ct -> do
          let utxoRow = getUtxoUpdate slotNo txs maybeTargetAddresses
          Ix.insert utxoRow index >>= innerLoop
        RollBackward cp _ct -> do
          events <- Ix.getEvents (index ^. Ix.storage)
          innerLoop $
            fromMaybe index $ do
              slot   <- chainPointToSlotNo cp
              offset <- findIndex  (\u -> (u ^. Utxo.slotNo) < slot) events
              Ix.rewind offset index

combinedIndexer
  :: Maybe FilePath
  -> Maybe TargetAddresses
  -> S.Stream (S.Of (ChainSyncEvent (BlockInMode CardanoMode))) IO r
  -> IO ()
combinedIndexer utxoPath maybeTargetAddresses = combineIndexers remainingIndexers
  where
    liftMaybe (worker, maybePath) = case maybePath of
      Just path -> Just (worker, path)
      _         -> Nothing
<<<<<<< HEAD
    pairs =
        [
            (utxoWorker maybeTargetAddresses, utxoPath)
            , (datumWorker, datumPath)
        ]
=======
    pairs = [(utxoWorker maybeTargetAddresses, utxoPath)]
>>>>>>> 14a02acf5 (Adapt Marconi.Indexers.Datum)
    remainingIndexers = mapMaybe liftMaybe pairs

combineIndexers
  :: [(Worker, FilePath)]
  -> S.Stream (S.Of (ChainSyncEvent (BlockInMode CardanoMode))) IO r
  -> IO ()
combineIndexers indexers = S.foldM_ step initial finish
  where
    initial :: IO Coordinator
    initial = do
      coordinator <- initialCoordinator $ length indexers
      mapM_ (uncurry (forkIndexer coordinator)) indexers
      pure coordinator

    step :: Coordinator -> ChainSyncEvent (BlockInMode CardanoMode) -> IO Coordinator
    step c@Coordinator{_barrier, _indexerCount, _channel} event = do
      waitQSemN _barrier _indexerCount
      atomically $ writeTChan _channel event
      pure c

    finish :: Coordinator -> IO ()
    finish _ = pure ()

forkIndexer :: Coordinator -> Worker -> FilePath -> IO ()
forkIndexer coordinator worker path = do
  ch <- atomically . dupTChan $ _channel coordinator
  void . forkIO . worker coordinator ch $ path

-- | Duplicated from cardano-api (not exposed in cardano-api)
-- This function should be removed when marconi will depend on a cardano-api version that has accepted this PR:
-- https://github.com/input-output-hk/cardano-node/pull/4569
txScriptValidityToScriptValidity :: C.TxScriptValidity era -> C.ScriptValidity
txScriptValidityToScriptValidity C.TxScriptValidityNone                = C.ScriptValid
txScriptValidityToScriptValidity (C.TxScriptValidity _ scriptValidity) = scriptValidity
