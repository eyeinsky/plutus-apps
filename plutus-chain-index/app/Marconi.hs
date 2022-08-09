{-# LANGUAGE GADTs               #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}
module Main where

import Cardano.Api (Block (Block), BlockHeader (BlockHeader), BlockInMode (BlockInMode), CardanoMode,
                    ChainPoint (ChainPoint, ChainPointAtGenesis), Hash, NetworkId (Mainnet, Testnet),
                    NetworkMagic (NetworkMagic), SlotNo (SlotNo), Tx (Tx), chainPointToSlotNo,
                    deserialiseFromRawBytesHex, proxyToAsType)
import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as Shelley
import Cardano.BM.Setup (withTrace)
import Cardano.BM.Trace (logError)
import Cardano.BM.Tracing (defaultConfigStdout)
import Cardano.Ledger.Alonzo.Scripts qualified as Alonzo
import Control.Concurrent (forkIO)
import Control.Concurrent.QSemN (QSemN, newQSemN, signalQSemN, waitQSemN)
import Control.Concurrent.STM (atomically)
import Control.Concurrent.STM.TChan (TChan, dupTChan, newBroadcastTChanIO, readTChan, writeTChan)
import Control.Exception (catch)
import Control.Lens.Operators ((&), (<&>), (^.))
import Control.Monad (void, when)
import Data.ByteString.Char8 qualified as C8
import Data.ByteString.Short qualified as SBS
import Data.Foldable (foldl')
import Data.List (findIndex)
import Data.Map (assocs)
import Data.Maybe (catMaybes, fromJust, fromMaybe, isJust)
import Data.Proxy (Proxy (Proxy))
import Data.Set (Set)
import Data.Set qualified as Set
import Data.String (IsString)
import Ledger (TxIn (..), TxOut (..), TxOutRef (..))
import Ledger.Scripts as Ledger
import Ledger.Tx.CardanoAPI (fromCardanoTxId, fromCardanoTxIn, fromCardanoTxOut, fromTxScriptValidity,
                             scriptDataFromCardanoTxBody, withIsCardanoEra)
import Marconi.Index.Datum (DatumIndex)
import Marconi.Index.Datum qualified as Datum
import Marconi.Index.ScriptTx ()
import Marconi.Index.ScriptTx qualified as ScriptTx
import Marconi.Index.Utxo (UtxoIndex, UtxoUpdate (..))
import Marconi.Index.Utxo qualified as Utxo
import Marconi.Logging (logging)
import Options.Applicative (Mod, OptionFields, Parser, auto, execParser, flag', help, helper, info, long, maybeReader,
                            metavar, option, readerError, strOption, (<**>), (<|>))
import Plutus.HystericalScreams.Index.VSplit qualified as Ix
import Plutus.Streaming (ChainSyncEvent (RollBackward, RollForward), ChainSyncEventException (NoIntersectionFound),
                         withChainSyncEventStream)
import Prettyprinter (defaultLayoutOptions, layoutPretty, pretty, (<+>))
import Prettyprinter.Render.Text (renderStrict)
import Streaming.Prelude qualified as S

-- | This executable is meant to exercise a set of indexers (for now datumhash -> datum)
--     against the mainnet (meant to be used for testing).
--
--     In case you want to access the results of the datumhash indexer you need to query
--     the resulting database:
--     $ sqlite3 datums.sqlite
--     > select slotNo, datumHash, datum from kv_datumhsh_datum where slotNo = 39920450;
--     39920450|679a55b523ff8d61942b2583b76e5d49498468164802ef1ebe513c685d6fb5c2|X(002f9787436835852ea78d3c45fc3d436b324184

data Options = Options
  { optionsSocketPath   :: String,
    optionsNetworkId    :: NetworkId,
    optionsChainPoint   :: ChainPoint,
    optionsUtxoPath     :: Maybe FilePath,
    optionsDatumPath    :: Maybe FilePath,
    optionsScriptTxPath :: Maybe FilePath
  }
  deriving (Show)

parseOptions :: IO Options
parseOptions = execParser $ info (optionsParser <**> helper) mempty

optionsParser :: Parser Options
optionsParser =
  Options
    <$> strOption (long "socket-path" <> help "Path to node socket.")
    <*> networkIdParser
    <*> chainPointParser
    <*> optStrParser (long "utxo-db" <> help "Path to the utxo database.")
    <*> optStrParser (long "datum-db" <> help "Path to the datum database.")
    <*> optStrParser (long "script-tx-db" <> help "Path to the script transactions' database.")

optStrParser :: IsString a => Mod OptionFields a -> Parser (Maybe a)
optStrParser fields = Just <$> strOption fields <|> pure Nothing

networkIdParser :: Parser NetworkId
networkIdParser =
  pMainnet <|> pTestnet
  where
    pMainnet =
      flag'
        Mainnet
        ( long "mainnet"
            <> help "Use the mainnet magic id."
        )

    pTestnet =
      Testnet . NetworkMagic
        <$> option
          auto
          ( long "testnet-magic"
              <> metavar "NATURAL"
              <> help "Specify a testnet magic id."
          )

chainPointParser :: Parser ChainPoint
chainPointParser =
  pure ChainPointAtGenesis
    <|> ( ChainPoint
            <$> option (SlotNo <$> auto) (long "slot-no" <> metavar "SLOT-NO")
            <*> option
              (maybeReader maybeParseHashBlockHeader <|> readerError "Malformed block hash")
              (long "block-hash" <> metavar "BLOCK-HASH")
        )

-- DatumIndexer
getDatums :: BlockInMode CardanoMode -> [(SlotNo, (DatumHash, Datum))]
getDatums (BlockInMode (Block (BlockHeader slotNo _ _) txs) _) = concatMap extractDatumsFromTx txs
  where
    extractDatumsFromTx
      :: Tx era
      -> [(SlotNo, (DatumHash, Datum))]
    extractDatumsFromTx (Tx txBody _) =
      let hashes = assocs . fst $ scriptDataFromCardanoTxBody txBody
       in map (slotNo,) hashes

-- UtxoIndexer
getOutputs
  :: C.Tx era
  -> Maybe [(TxOut, TxOutRef)]
getOutputs (C.Tx txBody@(C.TxBody C.TxBodyContent{C.txOuts}) _) = do
  outs <- either (const Nothing) Just $ traverse fromCardanoTxOut txOuts
  pure $ outs
    &  zip ([0..] :: [Integer])
   <&> (\(ix, out) -> (out, TxOutRef { txOutRefId  = fromCardanoTxId (C.getTxId txBody)
                                     , txOutRefIdx = ix
                                     }))

getInputs
  :: C.Tx era
  -> Set TxOutRef
getInputs (C.Tx (C.TxBody C.TxBodyContent{C.txIns, C.txScriptValidity, C.txInsCollateral}) _) =
  let isTxScriptValid = fromTxScriptValidity txScriptValidity
      inputs = if isTxScriptValid
                  then fst <$> txIns
                  else case txInsCollateral of
                    C.TxInsCollateralNone     -> []
                    C.TxInsCollateral _ txins -> txins
  in Set.fromList $ fmap (txInRef . (`TxIn` Nothing) . fromCardanoTxIn) inputs

getUtxoUpdate
  :: SlotNo
  -> [C.Tx era]
  -> UtxoUpdate
getUtxoUpdate slot txs =
  let ins  = foldl' Set.union Set.empty $ getInputs <$> txs
      outs = concat . catMaybes $ getOutputs <$> txs
  in  UtxoUpdate { _inputs  = ins
                 , _outputs = outs
                 , _slotNo  = slot
                 }

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

datumWorker
  :: Coordinator
  -> TChan (ChainSyncEvent (BlockInMode CardanoMode))
  -> FilePath
  -> IO ()
datumWorker Coordinator{_barrier} ch path = Datum.open path (Datum.Depth 2160) >>= innerLoop
  where
    innerLoop :: DatumIndex -> IO ()
    innerLoop index = do
      signalQSemN _barrier 1
      event <- atomically $ readTChan ch
      case event of
        RollForward blk _ct ->
          Ix.insert (getDatums blk) index >>= innerLoop
        RollBackward cp _ct -> do
          events <- Ix.getEvents (index ^. Ix.storage)
          innerLoop $
            fromMaybe index $ do
              slot   <- chainPointToSlotNo cp
              offset <- findIndex (any (\(s, _) -> s < slot)) events
              Ix.rewind offset index

utxoWorker
  :: Coordinator
  -> TChan (ChainSyncEvent (BlockInMode CardanoMode))
  -> FilePath
  -> IO ()
utxoWorker Coordinator{_barrier} ch path = Utxo.open path (Utxo.Depth 2160) >>= innerLoop
  where
    innerLoop :: UtxoIndex -> IO ()
    innerLoop index = do
      signalQSemN _barrier 1
      event <- atomically $ readTChan ch
      case event of
        RollForward (BlockInMode (Block (BlockHeader slotNo _ _) txs) _) _ct ->
          Ix.insert (getUtxoUpdate slotNo txs) index >>= innerLoop
        RollBackward cp _ct -> do
          events <- Ix.getEvents (index ^. Ix.storage)
          innerLoop $
            fromMaybe index $ do
              slot   <- chainPointToSlotNo cp
              offset <- findIndex  (\u -> (u ^. Utxo.slotNo) < slot) events
              Ix.rewind offset index

scriptTxWorker
  :: Coordinator
  -> TChan (ChainSyncEvent (BlockInMode CardanoMode))
  -> FilePath
  -> IO ()
scriptTxWorker Coordinator{_barrier} ch path = ScriptTx.open path (ScriptTx.Depth 2160) >>= loop
  where
    loop :: ScriptTx.ScriptTxIndex -> IO ()
    loop index = do
      signalQSemN _barrier 1
      event <- atomically $ readTChan ch
      case event of
        RollForward (BlockInMode (Block (BlockHeader slotNo _ _) txs :: Block era) era :: BlockInMode CardanoMode) _ct -> do
          withIsCardanoEra era (Ix.insert (toUpdate txs slotNo) index >>= loop)
        RollBackward cp _ct -> do
          events <- Ix.getEvents (index ^. Ix.storage)
          loop $
            fromMaybe index $ do
              slot   <- chainPointToSlotNo cp
              offset <- findIndex  (\u -> ScriptTx.slotNo u < slot) events
              Ix.rewind offset index

    toUpdate :: forall era . C.IsCardanoEra era => [Tx era] -> SlotNo -> ScriptTx.ScriptTxUpdate
    toUpdate txs slotNo = ScriptTx.ScriptTxUpdate txScripts' slotNo
      where
        txScripts' = map (\tx -> (txCbor tx, txScripts tx)) txs

    txCbor :: forall era . C.IsCardanoEra era => Tx era -> ScriptTx.TxCbor
    txCbor tx = ScriptTx.TxCbor $ C.serialiseToCBOR tx

    txScripts :: forall era . Tx era -> [ScriptTx.ScriptAddress]
    txScripts tx = let
        Tx (body :: C.TxBody era) _ws = tx
        hashesMaybe :: [Maybe C.ScriptHash]
        hashesMaybe = let
            _ = case body of
              C.TxBody (tbc :: C.TxBodyContent C.ViewTx era) -> undefined
          in case body of
          Shelley.ShelleyTxBody shelleyBasedEra _body scripts' _scriptData _auxData _validity ->
              case shelleyBasedEra of
                (C.ShelleyBasedEraAlonzo :: era0) -> map maybeScriptHash scripts'
                _                                 -> []
          _ -> []
        hashes = catMaybes hashesMaybe :: [Shelley.ScriptHash]
      in map ScriptTx.ScriptAddress hashes

    maybeScriptHash :: Alonzo.Script era1 -> Maybe Shelley.ScriptHash
    maybeScriptHash script = case script of
      Alonzo.PlutusScript _ (sbs :: SBS.ShortByteString) -> let

          -- | Use the ShortByteString to directly make a cardano-api script
          mkCardanoApiScript :: SBS.ShortByteString -> C.Script (C.PlutusScriptV1)
          mkCardanoApiScript = C.PlutusScript C.PlutusScriptV1 . Shelley.PlutusScriptSerialised

          hash = C.hashScript $ mkCardanoApiScript sbs :: C.ScriptHash
        in Just hash
      _ -> Nothing

combinedIndexer
  :: Maybe FilePath
  -> Maybe FilePath
  -> Maybe FilePath
  -> S.Stream (S.Of (ChainSyncEvent (BlockInMode CardanoMode))) IO r
  -> IO ()
combinedIndexer utxoPath datumPath scriptTxPath = S.foldM_ step initial finish
  where
    initial :: IO Coordinator
    initial = do
      let indexerCount = length . catMaybes $ [utxoPath, datumPath, scriptTxPath]
      coordinator <- initialCoordinator indexerCount
      when (isJust datumPath) $ do
        ch <- atomically . dupTChan $ _channel coordinator
        void . forkIO . datumWorker coordinator ch $ fromJust datumPath
      when (isJust utxoPath) $ do
        ch <- atomically . dupTChan $ _channel coordinator
        void . forkIO . utxoWorker coordinator ch $ fromJust utxoPath
      when (isJust scriptTxPath) $ do
        ch <- atomically . dupTChan $ _channel coordinator
        void . forkIO . scriptTxWorker coordinator ch $ fromJust scriptTxPath
      pure coordinator

    step :: Coordinator -> ChainSyncEvent (BlockInMode CardanoMode) -> IO Coordinator
    step c@Coordinator{_barrier, _indexerCount, _channel} event = do
      waitQSemN _barrier _indexerCount
      atomically $ writeTChan _channel event
      pure c

    finish :: Coordinator -> IO ()
    finish _ = pure ()

main :: IO ()
main = do
  Options { optionsSocketPath
          , optionsNetworkId
          , optionsChainPoint
          , optionsUtxoPath
          , optionsDatumPath
          , optionsScriptTxPath } <- parseOptions

  c <- defaultConfigStdout

  withTrace c "marconi" $ \trace ->
    withChainSyncEventStream
      optionsSocketPath
      optionsNetworkId
      optionsChainPoint
      (combinedIndexer optionsUtxoPath optionsDatumPath optionsScriptTxPath . logging trace)
      `catch` \NoIntersectionFound ->
        logError trace $
          renderStrict $
            layoutPretty defaultLayoutOptions $
              "No intersection found when looking for the chain point" <+> pretty optionsChainPoint <> "."
                <+> "Please check the slot number and the block hash do belong to the chain"

maybeParseHashBlockHeader :: String -> Maybe (Hash BlockHeader)
maybeParseHashBlockHeader = deserialiseFromRawBytesHex (proxyToAsType Proxy) . C8.pack
