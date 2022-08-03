{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TemplateHaskell            #-}


module Marconi.Index.ScriptTx where

import Cardano.Api (SlotNo)
import Cardano.Api.Shelley qualified as Shelley
import Codec.Serialise (deserialiseOrFail)
import Control.Lens.Operators ((^.))
import Data.ByteString qualified as BS

import Data.Foldable (forM_)
import Data.Maybe (fromJust)
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.FromField qualified as SQL
import Database.SQLite.Simple.ToField qualified as SQL
import GHC.Generics (Generic)
import Plutus.HystericalScreams.Index.VSqlite (SqliteIndex)
import Plutus.HystericalScreams.Index.VSqlite qualified as Ix


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
  fromField f = deserialiseOrFail <$> SQL.fromField f >>=
    either
      (\_ -> cantDeserialise)
      (\b -> maybe cantDeserialise (return . ScriptAddress) $ Shelley.deserialiseFromRawBytes Shelley.AsScriptHash b)
    where
      cantDeserialise = SQL.returnError SQL.ConversionFailed f "Cannot deserialise address."

instance SQL.ToRow ScriptTxRow where
  toRow o = [SQL.toField $ scriptAddress o, SQL.toField $ txCbor o]

-- * Indexer

type Query = ScriptAddress
type Result = [TxCbor]

data ScriptTxUpdate = ScriptTxUpdate
  { txScripts :: [(TxCbor, [ScriptAddress])]
  , slotNo    :: !SlotNo
  } deriving (Show)

type ScriptTxIndex = SqliteIndex ScriptTxUpdate () Query Result

open :: FilePath -> Depth -> IO ScriptTxIndex
open dbPath (Depth k) = do
  ix <- fromJust <$> Ix.newBoxed query store onInsert k ((k + 1) * 2) dbPath
  let c = ix ^. Ix.handle
  SQL.execute_ c "CREATE TABLE IF NOT EXISTS script_transactions (scriptAddress TEXT NOT NULL, txCbor BLOB NOT NULL)"
  SQL.execute_ c "CREATE INDEX IF NOT EXISTS script_address ON script_transactions (scriptAddress)"
  pure ix

  where
    onInsert :: ScriptTxIndex -> ScriptTxUpdate -> IO [()]
    onInsert _ix _update = pure []

    store :: ScriptTxIndex -> IO ()
    store ix = do
      persisted <- Ix.getEvents $ ix ^. Ix.storage
      buffered <- Ix.getBuffer $ ix ^. Ix.storage
      let updates = buffered ++ persisted :: [ScriptTxUpdate]
          rows = do
            ScriptTxUpdate txScriptAddrs _slotNo <- updates
            (txCbor', scriptAddrs) <- txScriptAddrs
            scriptAddr <- scriptAddrs
            pure $ ScriptTxRow scriptAddr txCbor'
      forM_ rows $
        SQL.execute (ix ^. Ix.handle) "INSERT INTO script_transactions (scriptAddress, txCbor) VALUES (?, ?)"

    query :: ScriptTxIndex -> Query -> [ScriptTxUpdate] -> IO Result
    query ix scriptAddress' updates = do
      persisted :: [SQL.Only TxCbor] <- SQL.query (ix ^. Ix.handle)
        "SELECT txCbor FROM utxos WHERE scriptAddress = ?" (SQL.Only scriptAddress')

      let
        buffered :: [TxCbor]
        buffered = do
          ScriptTxUpdate update _slotNo <- updates
          map fst $ filter (\(_, addrs) -> scriptAddress' `elem` addrs) update

        both :: [TxCbor]
        both = buffered <> map (\(SQL.Only txCbor') -> txCbor') persisted

      return both
