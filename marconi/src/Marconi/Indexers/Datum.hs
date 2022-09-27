{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module Marconi.Indexers.Datum where

import Codec.Serialise (deserialiseOrFail, serialise)
import Control.Monad.Trans.Class (lift)
import Data.ByteString.Lazy (toStrict)
import Data.Foldable (forM_)
import Data.String (fromString)
import Database.SQLite.Simple (SQLData (SQLBlob, SQLInteger, SQLText))
import Database.SQLite.Simple qualified as SQL
import Database.SQLite.Simple.FromField (FromField (fromField), ResultError (ConversionFailed), returnError)
import Database.SQLite.Simple.ToField (ToField (toField))
import Streaming.Prelude qualified as S

import Cardano.Api (SlotNo (SlotNo))
import Plutus.V1.Ledger.Api (Datum, DatumHash)

type DatumHash    = C.Hash C.ScriptData
type Event        = [(C.SlotNo, (DatumHash, C.ScriptData))]
type Query        = DatumHash
type Result       = Maybe Datum

newtype Depth = Depth Int

instance FromField DatumHash where
  fromField f = fromField f >>=
    maybe (returnError ConversionFailed f "Cannot deserialise datumhash.")
           pure
    . C.deserialiseFromRawBytes (C.AsHash C.AsScriptData)

instance ToField DatumHash where
  toField = SQLBlob . C.serialiseToRawBytes

instance Serialise C.ScriptData where
  encode = toCBOR
  decode = fromCBOR

instance FromField C.ScriptData where
  fromField f = fromField f >>=
    either (const $ returnError ConversionFailed f "Cannot deserialise datumhash.") pure
    . deserialiseOrFail

instance ToField C.ScriptData where
  toField = SQLBlob . toStrict . serialise

instance FromField C.SlotNo where
  fromField f = C.SlotNo <$> fromField f

instance ToField C.SlotNo where
  toField (C.SlotNo s) = SQLInteger $ fromIntegral s

sqlite :: FilePath -> S.Stream (S.Of Event) IO r -> S.Stream (S.Of Event) IO r
sqlite db source = do
  connection <- lift $ do
    c <- SQL.open db
    SQL.execute_ c "CREATE TABLE IF NOT EXISTS kv_datumhsh_datum (datumHash TEXT PRIMARY KEY, datum BLOB, slotNo INT)"
    pure c

  let
    unpack :: (SlotNo, (DatumHash, Datum)) -> (SlotNo, DatumHash, Datum, SlotNo)
    unpack (s, (h, d)) = (s, h, d, s)

    loop source' = lift (S.next source') >>= \case
      Left r -> pure r
      Right (event, source'') -> let
        event' = map unpack event
        in do
        lift $ do
          SQL.execute_ connection "BEGIN"
          forM_ event' $ SQL.execute connection
            "INSERT INTO kv_datumhsh_datum (slotNo, datumHash, datum) VALUES (?,?,?) ON CONFLICT(datumHash) DO UPDATE SET slotNo = ?"
          SQL.execute_ connection "COMMIT"
        S.yield event
        loop source''

  loop source
