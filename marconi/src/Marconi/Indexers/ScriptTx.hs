{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeOperators              #-}

module Marconi.Indexers.ScriptTx where

import Codec.Serialise (deserialiseOrFail)
import Control.Monad.Trans.Class (lift)
import Data.ByteString qualified as BS
import Data.Coerce (coerce)
import Data.Foldable (forM_, toList)
import Data.Function ((&))
import Data.Maybe (catMaybes)
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.FromField qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import GHC.Generics (Generic)
import Streaming.Prelude qualified as S

import Cardano.Api (SlotNo)
import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as Shelley
-- TODO Remove the following dependencies (and also cardano-ledger-*
-- package dependencies in cabal file) when fromShelleyBasedScript is
-- exported from cardano-node PR:
-- https://github.com/input-output-hk/cardano-node/pull/4386
import Cardano.Ledger.Alonzo.Language qualified as Alonzo
import Cardano.Ledger.Alonzo.Scripts qualified as Alonzo
import Cardano.Ledger.Core qualified
import Cardano.Ledger.Crypto qualified as LedgerCrypto
import Cardano.Ledger.Keys qualified as LedgerShelley
import Cardano.Ledger.Shelley.Scripts qualified as LedgerShelley
import Cardano.Ledger.ShelleyMA.Timelocks qualified as Timelock
import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))
import Ledger.Tx.CardanoAPI (withIsCardanoEra)

import Marconi.Streaming.ChainSync (chainEventSource, rollbackRingBuffer)

newtype Depth = Depth Int

newtype ScriptAddress = ScriptAddress Shelley.ScriptHash
  deriving (Show, Eq)
newtype TxCbor = TxCbor BS.ByteString
  deriving (Show)
  deriving newtype (SQL.ToField, SQL.FromField)

-- * SQLite

data ScriptTxRow = ScriptTxRow
  { scriptAddress :: !ScriptAddress
  , txCbor        :: !TxCbor
  } deriving (Generic)

instance SQL.ToField ScriptAddress where
  toField (ScriptAddress hash)  = SQL.SQLBlob . Shelley.serialiseToRawBytes $ hash
instance SQL.FromField ScriptAddress where
  fromField f = SQL.fromField f >>=
    either
      (const cantDeserialise)
      (\b -> maybe cantDeserialise (return . ScriptAddress) $ Shelley.deserialiseFromRawBytes Shelley.AsScriptHash b) . deserialiseOrFail
    where
      cantDeserialise = SQL.returnError SQL.ConversionFailed f "Cannot deserialise address."

instance SQL.ToRow ScriptTxRow where
  toRow o = [SQL.toField $ scriptAddress o, SQL.toField $ txCbor o]

data ScriptTxUpdate = ScriptTxUpdate
  { txScripts :: [(TxCbor, [ScriptAddress])]
  , slotNo    :: !SlotNo
  } deriving (Show)

toUpdate :: forall era . C.IsCardanoEra era => [C.Tx era] -> SlotNo -> ScriptTxUpdate
toUpdate txs = ScriptTxUpdate txScripts'
  where
    txScripts' = map (\tx -> (TxCbor $ C.serialiseToCBOR tx, getTxScripts tx)) txs

getTxBodyScripts :: forall era . C.TxBody era -> [ScriptAddress]
getTxBodyScripts body = let
    hashesMaybe :: [Maybe C.ScriptHash]
    hashesMaybe = case body of
      Shelley.ShelleyTxBody shelleyBasedEra _ scripts _ _ _ -> flip map scripts $ \script ->
        case fromShelleyBasedScript shelleyBasedEra script of
          Shelley.ScriptInEra _ script' -> Just $ C.hashScript script'
      _ -> [] -- Byron transactions have no scripts
    hashes = catMaybes hashesMaybe :: [Shelley.ScriptHash]
  in map ScriptAddress hashes

getTxScripts :: forall era . C.Tx era -> [ScriptAddress]
getTxScripts (C.Tx txBody _ws) = getTxBodyScripts txBody

-- | Convert ChainSyncEvent stream to a stream of ScriptTxUpdates
toScriptTx
  :: S.Stream (S.Of (ChainSyncEvent (C.BlockInMode C.CardanoMode))) IO r
  -> S.Stream (S.Of (ChainSyncEvent ScriptTxUpdate)) IO r
toScriptTx = S.map $ \case
  RollForward (C.BlockInMode (C.Block (C.BlockHeader slotNo' _ _) txs) era) ct -> do
    withIsCardanoEra era (RollForward (toUpdate txs slotNo') ct)
  RollBackward cp ct -> RollBackward cp ct

-- | Sqlite back-end for @ScriptTxUpdate@s. Persisted updates are yielded onwards.
sqlite :: FilePath -> S.Stream (S.Of ScriptTxUpdate) IO r -> S.Stream (S.Of ScriptTxUpdate) IO r
sqlite db source = do
  connection <- lift $ do
    c <- SQL.open db
    SQL.execute_ c "CREATE TABLE IF NOT EXISTS script_transactions (scriptAddress TEXT NOT NULL, txCbor BLOB NOT NULL)"
    SQL.execute_ c "CREATE INDEX IF NOT EXISTS script_address ON script_transactions (scriptAddress)"
    pure c

  let loop source' = lift (S.next source') >>= \case
        Left r -> pure r
        Right (scriptTxUpdate, source'') -> let
          ScriptTxUpdate txScriptAddrs _slotNo = scriptTxUpdate
          rows = do
            (txCbor', scriptAddrs) <- txScriptAddrs
            scriptAddr <- scriptAddrs
            pure $ ScriptTxRow scriptAddr txCbor'
          in do
          lift $ forM_ rows $ SQL.execute connection "INSERT INTO script_transactions (scriptAddress, txCbor) VALUES (?, ?)"
          S.yield scriptTxUpdate
          loop source''

  loop source

query :: FilePath -> ScriptAddress -> IO [TxCbor]
query db scriptAddress' = do
  c <- SQL.open db
  persisted :: [SQL.Only TxCbor] <- SQL.query c
    "SELECT txCbor FROM script_transactions WHERE scriptAddress = ?" (SQL.Only scriptAddress')
  return $ map coerce persisted


{- | The indexer itself:
       - source chain sync events from @socket@
       - buffer them until @n@
       - write them to an sqlite database at @db@
-}
indexer :: FilePath -> C.NetworkId -> Shelley.ChainPoint -> FilePath -> IO ()
indexer socket networkId chainPoint db = chainEventSource socket networkId chainPoint
  & toScriptTx
  & rollbackRingBuffer 2160
  & sqlite db
  & S.effects

-- * Copy-paste
--
-- | TODO: Remove when the following function is exported from Cardano.Api.Script
-- PR: https://github.com/input-output-hk/cardano-node/pull/4386
fromShelleyBasedScript  :: Shelley.ShelleyBasedEra era
                        -> Cardano.Ledger.Core.Script (Shelley.ShelleyLedgerEra era)
                        -> Shelley.ScriptInEra era
fromShelleyBasedScript era script =
  case era of
    Shelley.ShelleyBasedEraShelley ->
      Shelley.ScriptInEra Shelley.SimpleScriptV1InShelley $
      Shelley.SimpleScript Shelley.SimpleScriptV1 $
      fromShelleyMultiSig script
    Shelley.ShelleyBasedEraAllegra ->
      Shelley.ScriptInEra Shelley.SimpleScriptV2InAllegra $
      Shelley.SimpleScript Shelley.SimpleScriptV2 $
      fromAllegraTimelock Shelley.TimeLocksInSimpleScriptV2 script
    Shelley.ShelleyBasedEraMary ->
      Shelley.ScriptInEra Shelley.SimpleScriptV2InMary $
      Shelley.SimpleScript Shelley.SimpleScriptV2 $
      fromAllegraTimelock Shelley.TimeLocksInSimpleScriptV2 script
    Shelley.ShelleyBasedEraAlonzo ->
      case script of
        Alonzo.TimelockScript s ->
          Shelley.ScriptInEra Shelley.SimpleScriptV2InAlonzo $
          Shelley.SimpleScript Shelley.SimpleScriptV2 $
          fromAllegraTimelock Shelley.TimeLocksInSimpleScriptV2 s
        Alonzo.PlutusScript Alonzo.PlutusV1 s ->
          Shelley.ScriptInEra Shelley.PlutusScriptV1InAlonzo $
          Shelley.PlutusScript Shelley.PlutusScriptV1 $
          Shelley.PlutusScriptSerialised s
        Alonzo.PlutusScript Alonzo.PlutusV2 s ->
          Shelley.ScriptInEra Shelley.PlutusScriptV2InAlonzo $
          Shelley.PlutusScript Shelley.PlutusScriptV2 $
          Shelley.PlutusScriptSerialised  s

  where
  fromAllegraTimelock :: Shelley.TimeLocksSupported lang
                      -> Timelock.Timelock LedgerCrypto.StandardCrypto
                      -> Shelley.SimpleScript lang
  fromAllegraTimelock timelocks = go
    where
      go (Timelock.RequireSignature kh) = Shelley.RequireSignature
                                            (Shelley.PaymentKeyHash (LedgerShelley.coerceKeyRole kh))
      go (Timelock.RequireTimeExpire t) = Shelley.RequireTimeBefore timelocks t
      go (Timelock.RequireTimeStart  t) = Shelley.RequireTimeAfter  timelocks t
      go (Timelock.RequireAllOf      s) = Shelley.RequireAllOf (map go (toList s))
      go (Timelock.RequireAnyOf      s) = Shelley.RequireAnyOf (map go (toList s))
      go (Timelock.RequireMOf      i s) = Shelley.RequireMOf i (map go (toList s))

  fromShelleyMultiSig :: LedgerShelley.MultiSig LedgerCrypto.StandardCrypto -> Shelley.SimpleScript lang
  fromShelleyMultiSig = go
    where
      go (LedgerShelley.RequireSignature kh)
                                  = Shelley.RequireSignature
                                      (Shelley.PaymentKeyHash (LedgerShelley.coerceKeyRole kh))
      go (LedgerShelley.RequireAllOf s) = Shelley.RequireAllOf (map go s)
      go (LedgerShelley.RequireAnyOf s) = Shelley.RequireAnyOf (map go s)
      go (LedgerShelley.RequireMOf m s) = Shelley.RequireMOf m (map go s)
