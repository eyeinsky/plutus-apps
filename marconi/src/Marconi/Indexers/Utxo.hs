{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE GADTs              #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE NamedFieldPuns     #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE PatternSynonyms    #-}
{-# LANGUAGE TemplateHaskell    #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Marconi.Indexers.Utxo
  ( -- * UtxoIndex
    UtxoIndex
  , Depth(..)
  , open
  , Ix.insert
  , Ix.rewind
  , UtxoUpdate(..)
  , UtxoRow(..)
  , toRows
  , inputs
  , outputs
  , slotNo
  , address
  , reference
  , TxOut
  ) where

import Cardano.Api (SlotNo, TxIn (TxIn))
import Cardano.Api qualified as C

import Control.Lens.Operators ((&), (^.))
import Control.Lens.TH (makeLenses)

import Control.Monad (when)
import Control.Monad.Trans.Class (lift)
import Data.ByteString.Lazy (toStrict)
import Data.Foldable (foldl', forM_, toList)
import Data.List.NonEmpty qualified as NonEmpty
import Data.Set (Set)
import Data.Set qualified as Set
import Data.String (fromString)
import Database.SQLite.Simple (SQLData (SQLBlob, SQLText))
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.FromField (FromField (fromField), ResultError (ConversionFailed), returnError)
import Database.SQLite.Simple.FromRow (FromRow (fromRow), field)
import Database.SQLite.Simple.ToField (ToField (toField))
import Database.SQLite.Simple.ToRow (ToRow (toRow))
import GHC.Generics (Generic)
import Streaming.Prelude qualified as S
import Cardano.Api qualified as C
import Control.Lens.Operators ((<&>))
import Data.Maybe (catMaybes)
import Ledger (Address, TxId, TxIn (TxIn), TxOut, TxOutRef (TxOutRef, txOutRefId, txOutRefIdx), txInRef)
import Ledger qualified as Ledger
import Ledger.Tx.CardanoAPI (fromCardanoTxId, fromCardanoTxIn, fromCardanoTxOut, fromTxScriptValidity)
import Marconi.Streaming.ChainSync (chainEventSource, rollbackRingBuffer)
import System.Random.MWC (createSystemRandom, uniformR)


data UtxoUpdate = UtxoUpdate
  { _inputs  :: !(Set TxIn)
  , _outputs :: ![(TxOut, TxOutRef)]
  , _slotNo  :: !SlotNo
  } deriving (Show)

$(makeLenses ''UtxoUpdate)

type Result = Maybe [TxOutRef]

type UtxoIndex = SqliteIndex UtxoUpdate () C.AddressAny Result

newtype Depth = Depth Int

instance FromField C.AddressAny where
  fromField f = fromField f >>=
    maybe (returnError ConversionFailed f "Cannot deserialise address.")
          pure
    . C.deserialiseFromRawBytes C.AsAddressAny

instance ToField C.AddressAny where
  toField = SQLBlob . C.serialiseToRawBytes

instance FromField C.TxId where
  fromField = fmap fromString . fromField

instance ToField C.TxId where
  toField = SQLText . fromString . show

instance FromField C.TxIx where
  fromField = fmap C.TxIx . fromField

instance ToField C.TxIx where
  toField (C.TxIx i) = SQLInteger $ fromIntegral i

data UtxoRow = UtxoRow
  { _address   :: !C.AddressAny
  , _reference :: !TxOutRef
  } deriving (Generic)

$(makeLenses ''UtxoRow)

instance FromRow UtxoRow where
  fromRow = UtxoRow <$> field <*> (txOutRef <$> field <*> field)

instance ToRow UtxoRow where
  toRow u =  (toField $ u ^. address) : (toRow $ u ^. reference)

instance FromRow TxOutRef where
  fromRow = txOutRef <$> field <*> field

instance ToRow TxOutRef where
  toRow (TxIn txOutRefId txOutRefIdx) =
      [ toField txOutRefId
      , toField txOutRefIdx
      ]

getOutputs
  :: Maybe TargetAddresses
  -> C.Tx era
  -> Maybe [(TxOut, TxOutRef)]
getOutputs maybeTargetAddresses (C.Tx txBody@(C.TxBody C.TxBodyContent{C.txOuts}) _) = do
    outs <- case maybeTargetAddresses of
        Just targetAddresses ->
            either (const Nothing) Just $ traverse fromCardanoTxOut . filter (isTargetTxOut targetAddresses) $ txOuts
        Nothing ->
            either (const Nothing) Just $ traverse fromCardanoTxOut  txOuts
    pure $ outs &  zip ([0..] :: [Integer])
        <&> (\(ix, out) -> (out, TxOutRef { txOutRefId  = fromCardanoTxId (C.getTxId txBody)
                                          , txOutRefIdx = ix
                                     }))

getInputs
  :: C.Tx era
  -> Set TxOutRef
getInputs (C.Tx (C.TxBody C.TxBodyContent{C.txIns, C.txScriptValidity, C.txInsCollateral}) _) =
  let isTxScriptValid = fromTxScriptValidity txScriptValidity
      inputs' = if isTxScriptValid
                  then fst <$> txIns
                  else case txInsCollateral of
                    C.TxInsCollateralNone     -> []
                    C.TxInsCollateral _ txins -> txins
  in Set.fromList $ fmap (txInRef . (`TxIn` Nothing) . fromCardanoTxIn) inputs'

getUtxoUpdate
  :: SlotNo
  -> [C.Tx era]
  -> Maybe TargetAddresses
  -> UtxoUpdate
getUtxoUpdate slot txs addresses =
  let ins  = foldl' Set.union Set.empty $ getInputs <$> txs
      outs = concat . catMaybes $ getOutputs addresses <$> txs
  in  UtxoUpdate { _inputs  = ins
                 , _outputs = outs
                 , _slotNo  = slot
                 }

sqlite :: FilePath -> S.Stream (S.Of UtxoUpdate) IO r -> S.Stream (S.Of UtxoUpdate) IO r
sqlite db source = do
  c <- lift $ do
    c' <- SQL.open db
    SQL.execute_ c' "CREATE TABLE IF NOT EXISTS utxos (address TEXT NOT NULL, txId TEXT NOT NULL, inputIx INT NOT NULL)"
    SQL.execute_ c' "CREATE TABLE IF NOT EXISTS spent (txId TEXT NOT NULL, inputIx INT NOT NULL)"
    SQL.execute_ c' "CREATE INDEX IF NOT EXISTS utxo_address ON utxos (address)"
    SQL.execute_ c' "CREATE INDEX IF NOT EXISTS utxo_refs ON utxos (txId, inputIx)"
    SQL.execute_ c' "CREATE INDEX IF NOT EXISTS spent_refs ON spent (txId, inputIx)"
    pure c'

  let loop source' = lift (S.next source') >>= \case
        Left r -> pure r
        Right (utxoUpdate, source'') -> let
            utxos = toRows utxoUpdate
            spent = toList $ _inputs utxoUpdate
          in do
          lift $ do
            SQL.execute_ c "BEGIN"
            forM_ utxos $
              SQL.execute c "INSERT INTO utxos (address, txId, inputIx) VALUES (?, ?, ?)"
            forM_ spent $
              SQL.execute c "INSERT INTO spent (txId, inputIx) VALUES (?, ?)"
            SQL.execute_ c "COMMIT"

            -- We want to perform vacuum about once every 100 * buffer ((k + 1) * 2)
            rndCheck <- createSystemRandom >>= uniformR (1 :: Int, 100)
            when (rndCheck == 42) $ do
              SQL.execute_ c "DELETE FROM utxos WHERE utxos.rowid IN (SELECT utxos.rowid FROM utxos LEFT JOIN spent on utxos.txId = spent.txId AND utxos.inputIx = spent.inputIx WHERE spent.txId IS NOT NULL)"
              SQL.execute_ c "VACUUM"

          S.yield utxoUpdate
          loop source''

  loop source

  where
    toRows :: UtxoUpdate -> [UtxoRow]
    toRows update = update ^. outputs
      & map (\(out, ref)  -> UtxoRow { _address   = Ledger.txOutAddress out
                                     , _reference = ref
                                     })

indexer :: FilePath -> C.NetworkId -> C.ChainPoint -> FilePath -> Maybe TargetAddresses -> IO ()
indexer socket networkId chainPoint db maybeTargetAddresses = chainEventSource socket networkId chainPoint
  & rollbackRingBuffer 2160
  & S.map toUtxoUpdate
  & sqlite db
  & S.effects
  where
    toUtxoUpdate :: C.BlockInMode C.CardanoMode -> UtxoUpdate
    toUtxoUpdate (C.BlockInMode (C.Block (C.BlockHeader slotNo' _ _) txs) _) =
      getUtxoUpdate slotNo' txs maybeTargetAddresses

-- * Filter by target address

type TargetAddresses = NonEmpty.NonEmpty (C.Address C.ShelleyAddr)

isTargetTxOut :: TargetAddresses -> C.TxOut C.CtxTx era -> Bool
isTargetTxOut targetAddresses (C.TxOut address' _ _) = case  address' of
    (C.AddressInEra  (C.ShelleyAddressInEra _) addr) -> addr `elem` targetAddresses
    _                                                -> False
