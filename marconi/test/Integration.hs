{-# LANGUAGE NamedFieldPuns     #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings  #-}
{-# LANGUAGE TypeApplications   #-}
{-# OPTIONS_GHC -Wno-unused-imports #-}
{-# OPTIONS_GHC -Wno-missing-import-lists #-}

module Integration where

import Control.Concurrent qualified as IO
import Control.Exception (catch)
import Control.Monad (void)
import Data.Aeson qualified as Aeson
import Data.ByteString.Builder qualified as BS
import Data.Char (toLower)
import Data.HashMap.Lazy (HashMap)
import Data.HashMap.Lazy qualified as HM
import Data.Monoid (Last (Last))
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Encoding qualified as TL
import Hedgehog (MonadTest, Property, assert, eval, (===))
import Hedgehog qualified as H
import Hedgehog.Extras.Stock.IO.Network.Sprocket qualified as IO
import Hedgehog.Extras.Test qualified as HE
import Hedgehog.Extras.Test.Base qualified as H
import Hedgehog.Extras.Test.Concurrent qualified as H
import Hedgehog.Extras.Test.File qualified as H
import Hedgehog.Extras.Test.Process qualified as H
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Prettyprinter.Render.Text (renderStrict)
import System.Directory qualified as IO
import System.Environment qualified as IO
import System.FilePath ((</>))
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Hedgehog (testProperty)

import Cardano.Api (CardanoEra (AlonzoEra))
import Cardano.Api qualified as C
import Cardano.Api.Byron qualified as C
import Cardano.Api.Shelley qualified as C
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson ((.:))
import Data.Function ((&))
import Data.Functor ((<&>))
import Data.Map qualified as Map
import Data.Text (Text)
import Gen.Cardano.Api.Typed qualified as CGen
import Ouroboros.Network.Protocol.LocalTxSubmission.Type
import Test.Base qualified as H
import Test.Base qualified as T
import Test.Process qualified as H
import Testnet.Cardano qualified as H
import Testnet.Cardano qualified as TN
import Testnet.Conf qualified as H
import Testnet.Conf qualified as TC (Conf (..), ProjectBase (ProjectBase), YamlFilePath (YamlFilePath), mkConf)
import Testnet.SubmitApi qualified as TN

import Cardano.BM.Setup (withTrace)
import Cardano.BM.Trace (logError)
import Cardano.BM.Tracing (defaultConfigStdout)
import Database.SQLite.Simple qualified as Sql
import Marconi.Indexers qualified
import Marconi.Logging qualified
import Plutus.Streaming (ChainSyncEventException (NoIntersectionFound), withChainSyncEventStream)
import Prettyprinter (defaultLayoutOptions, layoutPretty, pretty, (<+>))

-- Copied from plutus-example/test/Test/PlutusExample/SubmitApi/TxInLockingPlutus
data Utxo = Utxo
  { address :: Text
  , value   :: HashMap Text Integer
  } deriving (Eq, Show)

instance Aeson.FromJSON Utxo where
  parseJSON = Aeson.withObject "Utxo" $ \v -> Utxo
    <$> v .: "address"
    <*> v .: "value"

-- * Tmp

p :: (MonadIO m) => String -> m ()
p = liftIO . putStrLn

p2 :: (Show a, MonadIO m) => String -> a -> m ()
p2 str a = liftIO $ putStrLn $ str <> ": " <> show a

pause :: MonadIO m => m ()
pause = liftIO readLn

exit :: String -> m ()
exit = error

exit2 :: Show a => String -> a -> m ()
exit2 label v = exit $ label <> ": " <> show v

exit_ :: m ()
exit_ = exit "MANUAL EXIT"

main :: IO ()
main = defaultMain tests

-- /Tmp

readAs :: (C.HasTextEnvelope a, MonadIO m, MonadTest m) => C.AsType a -> FilePath -> m a
readAs as path = H.leftFailM . liftIO $ C.readFileTextEnvelope as path

findUTxOByAddress
  :: (MonadIO m, MonadTest m)
  => C.LocalNodeConnectInfo C.CardanoMode -> C.AddressAny -> m (C.UTxO C.AlonzoEra)
findUTxOByAddress localNodeConnectInfo address = let
  query = C.QueryInShelleyBasedEra C.ShelleyBasedEraAlonzo $ C.QueryUTxO $
    C.QueryUTxOByAddress $ Set.singleton address
  in
  H.leftFailM . H.leftFailM . liftIO $ C.queryNodeLocalState localNodeConnectInfo Nothing $
    C.QueryInEra C.AlonzoEraInCardanoMode query

failOnTxSubmitFail :: (Show a, Applicative m) => SubmitResult a -> m ()
failOnTxSubmitFail submitResult =
  case submitResult of
    SubmitFail reason -> exit2 "reason" reason
    SubmitSuccess     -> pure ()


tests :: TestTree
tests = testGroup "Integration"
  [ testProperty "prop_script_hashes_in_tx_match" testIndex ]

testIndex :: Property
testIndex = T.integration . HE.runFinallies . HE.workspace "chairman" $ \tempAbsBasePath' -> do

  base <- HE.noteM $ liftIO . IO.canonicalizePath =<< HE.getProjectBase

  configurationTemplate <- H.noteShow $ base </> "configuration/defaults/byron-mainnet/configuration.yaml"
  conf@TC.Conf { TC.tempBaseAbsPath, TC.tempAbsPath } <- HE.noteShowM $
    TC.mkConf (TC.ProjectBase base) (TC.YamlFilePath configurationTemplate)
      (tempAbsBasePath' <> "/")
      Nothing

  H.threadDelay 10000

  TN.TestnetRuntime { TN.bftSprockets, TN.testnetMagic } <- TN.testnet TN.defaultTestnetOptions conf
  let networkId = C.Testnet $ C.NetworkMagic $ fromIntegral testnetMagic

  let socketPath = IO.sprocketArgumentName (head bftSprockets)
  socketPathAbs <- H.note $ tempAbsPath </> socketPath

  H.threadDelay 1_000_000

  -- Start indexer
  let sqlitedb = tempAbsPath </> "script-tx.db"
  void $ liftIO $ IO.forkIO $ do
    let chainPoint = C.ChainPointAtGenesis :: C.ChainPoint
    c <- defaultConfigStdout
    withTrace c "marconi" $ \trace ->
      withChainSyncEventStream
        socketPathAbs
        networkId
        chainPoint
        (Marconi.Indexers.combinedIndexer Nothing Nothing (Just sqlitedb) . Marconi.Logging.logging trace)
        `catch` \NoIntersectionFound ->
          logError trace $
            renderStrict $
              layoutPretty defaultLayoutOptions $
                "No intersection found when looking for the chain point" <+> pretty chainPoint <> "."
                  <+> "Please check the slot number and the block hash do belong to the chain"

  assert $ tempAbsPath == (tempAbsBasePath' <> "/")
        && tempAbsPath == (tempBaseAbsPath <> "/")

  utxoVKeyFile <- H.note $ tempAbsPath </> "shelley/utxo-keys/utxo1.vkey"
  utxoSKeyFile <- H.note $ tempAbsPath </> "shelley/utxo-keys/utxo1.skey"

  -- Create the Shelley Address from the actual Plutus script.
  plutusScriptFileInUse <- H.note $ base </> "plutus-example/plutus/scripts/always-succeeds-spending.plutus"
  plutusScript <- C.PlutusScript C.PlutusScriptV1
    <$> readAs (C.AsPlutusScript C.AsPlutusScriptV1) plutusScriptFileInUse
  let
    plutusScriptHash :: C.ScriptHash
    plutusScriptHash = C.hashScript plutusScript
    plutusScriptAddr :: C.Address C.ShelleyAddr
    plutusScriptAddr = C.makeShelleyAddress networkId (C.PaymentCredentialByScript plutusScriptHash) C.NoStakeAddress

  -- Always succeeds Plutus script in use. Any datum and redeemer combination will succeed.
  -- Script at: $plutusscriptinuse

  -- Step 1: Create a tx ouput with a datum hash at the script address. In order for a tx ouput to be locked
  -- by a plutus script, it must have a datahash. We also need collateral tx inputs so we split the utxo
  -- in order to accomodate this.

  genesisVKey :: C.VerificationKey C.GenesisUTxOKey <-
    readAs (C.AsVerificationKey C.AsGenesisUTxOKey) utxoVKeyFile

  genesisSKey :: C.SigningKey C.GenesisUTxOKey <- readAs (C.AsSigningKey C.AsGenesisUTxOKey) utxoSKeyFile

  let
    paymentKey = C.castVerificationKey genesisVKey :: C.VerificationKey C.PaymentKey
    address :: C.Address C.ShelleyAddr
    address = C.makeShelleyAddress
      networkId
      (C.PaymentCredentialByKey (C.verificationKeyHash paymentKey :: C.Hash C.PaymentKey))
      C.NoStakeAddress :: C.Address C.ShelleyAddr

  -- Boilerplate codecs used for protocol serialisation.  The number
  -- of epochSlots is specific to each blockchain instance. This value
  -- what the cardano main and testnet uses. Only applies to the Byron
  -- era.
  let epochSlots = C.EpochSlots 21600
      localNodeConnectInfo =
          C.LocalNodeConnectInfo
              { C.localConsensusModeParams = C.CardanoModeParams epochSlots
              , C.localNodeNetworkId = networkId
              , C.localNodeSocketPath = socketPathAbs
              }

  utxo <- findUTxOByAddress localNodeConnectInfo $ C.toAddressAny address

  let utxoMap = C.unUTxO utxo
  txIn <- H.noteShow $ head $ Map.keys utxoMap

  (C.Lovelace lovelaceAtTxin) <- H.nothingFailM . H.noteShow $ (\(C.TxOut _ v _) -> C.txOutValueToLovelace v) <$> (utxoMap & Map.lookup txIn)
  lovelaceAtTxinDiv3 <- H.noteShow $ lovelaceAtTxin `div` 3

  pparams <- H.leftFailM . H.leftFailM . liftIO
    $ C.queryNodeLocalState localNodeConnectInfo Nothing
    $ C.QueryInEra C.AlonzoEraInCardanoMode
    $ C.QueryInShelleyBasedEra C.ShelleyBasedEraAlonzo C.QueryProtocolParameters

  let scriptDatum = C.ScriptDataNumber 42 :: C.ScriptData
      scriptDatumHash = C.hashScriptData scriptDatum
  p2 "scriptDatumHash" scriptDatumHash

  -- TODO [X] Create with Cardano.Api.TxBody instead of the CLI
  let
      txOut1 :: C.TxOut ctx C.AlonzoEra
      txOut1 =
          C.TxOut
            (C.AddressInEra (C.ShelleyAddressInEra C.ShelleyBasedEraAlonzo) plutusScriptAddr)
            (C.TxOutValue C.MultiAssetInAlonzoEra $ C.lovelaceToValue $ C.Lovelace lovelaceAtTxinDiv3)
            (C.TxOutDatumHash C.ScriptDataInAlonzoEra scriptDatumHash)
      txOut2 :: C.TxOut ctx C.AlonzoEra
      txOut2 =
          C.TxOut
            (C.AddressInEra (C.ShelleyAddressInEra C.ShelleyBasedEraAlonzo) address)
            (C.TxOutValue C.MultiAssetInAlonzoEra $ C.lovelaceToValue $ C.Lovelace $ 2 * lovelaceAtTxinDiv3 - 271)
            C.TxOutDatumNone
      txBodyContent :: C.TxBodyContent C.BuildTx C.AlonzoEra
      txBodyContent =
        C.TxBodyContent {
          C.txIns              = [(txIn, C.BuildTxWith $ C.KeyWitness C.KeyWitnessForSpending)],
          C.txInsCollateral    = C.TxInsCollateralNone,
          C.txOuts             = [txOut1, txOut2],
          C.txFee              = C.TxFeeExplicit C.TxFeesExplicitInAlonzoEra $ C.Lovelace 271,
          C.txValidityRange    = (C.TxValidityNoLowerBound, C.TxValidityNoUpperBound C.ValidityNoUpperBoundInAlonzoEra),
          C.txMetadata         = C.TxMetadataNone,
          C.txAuxScripts       = C.TxAuxScriptsNone,
          C.txExtraKeyWits     = C.TxExtraKeyWitnessesNone,
          C.txProtocolParams   = C.BuildTxWith $ Just pparams,
          C.txWithdrawals      = C.TxWithdrawalsNone,
          C.txCertificates     = C.TxCertificatesNone,
          C.txUpdateProposal   = C.TxUpdateProposalNone,
          C.txMintValue        = C.TxMintNone,
          C.txScriptValidity   = C.TxScriptValidityNone
        }

  tx1body :: C.TxBody C.AlonzoEra <- H.leftFail $ C.makeTransactionBody txBodyContent
  p2 "tx1body" tx1body
  let
    kw :: C.KeyWitness C.AlonzoEra
    kw = C.makeShelleyKeyWitness tx1body (C.WitnessPaymentKey $ C.castSigningKey genesisSKey)
    tx1 = C.makeSignedTransaction [kw] tx1body

  submitResult :: SubmitResult (C.TxValidationErrorInMode C.CardanoMode) <-
    liftIO $ C.submitTxToNodeLocal localNodeConnectInfo $ C.TxInMode tx1 C.AlonzoEraInCardanoMode
  failOnTxSubmitFail submitResult

  -- Second transaction: spend the UTXO specified in the first transaction

  HE.threadDelay 2_000_000 -- wait for the first transaction to be accepted

  txIn_ <- head . Map.keys . C.unUTxO <$> findUTxOByAddress localNodeConnectInfo (C.toAddressAny address)

  scriptUtxo <- findUTxOByAddress localNodeConnectInfo $ C.toAddressAny plutusScriptAddr
  scriptTxIn <- H.noteShow $ head $ Map.keys $ C.unUTxO scriptUtxo

  redeemer <- H.forAll CGen.genScriptData
  let
      executionUnits = C.ExecutionUnits {C.executionSteps = 500000,C.executionMemory = 500000 }
      fee = 100343

      C.PlutusScript _lang plutusScript_ = plutusScript
      scriptWitness :: C.Witness C.WitCtxTxIn C.AlonzoEra
      scriptWitness = C.ScriptWitness C.ScriptWitnessForSpending $
        C.PlutusScriptWitness C.PlutusScriptV1InAlonzo C.PlutusScriptV1 plutusScript_
        (C.ScriptDatumForTxIn scriptDatum) redeemer executionUnits

      collateral = C.TxInsCollateral C.CollateralInAlonzoEra [txIn_]

      tx2out :: C.TxOut ctx C.AlonzoEra
      tx2out =
          C.TxOut
            (C.AddressInEra (C.ShelleyAddressInEra C.ShelleyBasedEraAlonzo) address)
             -- send ADA back to the original genesis address               ^
            (C.TxOutValue C.MultiAssetInAlonzoEra $ C.lovelaceToValue $ C.Lovelace $ lovelaceAtTxinDiv3 - fee)
            C.TxOutDatumNone

      tx2bodyContent :: C.TxBodyContent C.BuildTx C.AlonzoEra
      tx2bodyContent =
        C.TxBodyContent {
          C.txIns              = [(scriptTxIn, C.BuildTxWith scriptWitness)],
          C.txInsCollateral    = collateral,
          C.txOuts             = [tx2out],
          C.txFee              = C.TxFeeExplicit C.TxFeesExplicitInAlonzoEra $ C.Lovelace fee,
          C.txValidityRange    = (C.TxValidityNoLowerBound, C.TxValidityNoUpperBound C.ValidityNoUpperBoundInAlonzoEra),
          C.txMetadata         = C.TxMetadataNone,
          C.txAuxScripts       = C.TxAuxScriptsNone,
          C.txExtraKeyWits     = C.TxExtraKeyWitnessesNone,
          C.txProtocolParams   = C.BuildTxWith $ Just pparams,
          C.txWithdrawals      = C.TxWithdrawalsNone,
          C.txCertificates     = C.TxCertificatesNone,
          C.txUpdateProposal   = C.TxUpdateProposalNone,
          C.txMintValue        = C.TxMintNone,
          C.txScriptValidity   = C.TxScriptValidityNone
        }

  tx2body :: C.TxBody C.AlonzoEra <- H.leftFail $ C.makeTransactionBody tx2bodyContent
  let tx2 = C.signShelleyTransaction tx2body [C.WitnessGenesisUTxOKey genesisSKey]

  tx2submitResult <- liftIO $ C.submitTxToNodeLocal localNodeConnectInfo $ C.TxInMode tx2 C.AlonzoEraInCardanoMode
  failOnTxSubmitFail tx2submitResult

  H.threadDelay 1_500_000

  r :: String <- liftIO $ do
    sqlConnection <- Sql.open sqlitedb
    (Sql.Only r : _) <- Sql.query_ sqlConnection "SELECT hex(scriptAddress) FROM script_transactions"
    pure r

  let lhs = map toLower r
      rhs = TL.unpack $ TL.decodeUtf8 $ BS.toLazyByteString $ BS.byteStringHex $ C.serialiseToRawBytes plutusScriptHash
  lhs === rhs
