{-# LANGUAGE NamedFieldPuns #-}
{-# LANGUAGE TupleSections  #-}

module MarconiSpec where

import Control.Arrow
import Control.Monad (replicateM)
import Data.Coerce

import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

import Cardano.Api
import Cardano.Api qualified as Api
import Gen.Cardano.Api.Metadata qualified as CGen
import Gen.Cardano.Api.Typed qualified as CGen

import Marconi.Index.ScriptTx qualified as ScriptTx

tests :: TestTree
tests = testGroup "Marconi"
  [ testProperty "prop_script_hashes_in_tx_match" getTxBodyScriptsRoundtrip ]

getTxBodyScriptsRoundtrip :: Property
getTxBodyScriptsRoundtrip = property $ do
  nScripts <- forAll $ Gen.integral (Range.linear 5 500)
  txInsAndHashes <- replicateM nScripts $ forAll genPlutusScriptTxIn
  let (txIns, scriptHashes) = unzip txInsAndHashes
  txBody <- forAll $ genTxBodyWithTxIns AlonzoEra $ map (second BuildTxWith) txIns
  let hashesFound = map coerce $ ScriptTx.getTxBodyScripts txBody :: [ScriptHash]
  scriptHashes === hashesFound

genTxBodyWithTxIns
  :: IsCardanoEra era
  => CardanoEra era
  -> [(TxIn, BuildTxWith BuildTx (Witness WitCtxTxIn era))]
  -> Gen (TxBody era)
genTxBodyWithTxIns era txIns = do
  txBodyContent <- genTxBodyContentWithTxIns era txIns
  let eitherTxBody = makeTransactionBody txBodyContent
  case eitherTxBody of
    Left err     -> fail $ displayError err
    Right txBody -> pure txBody

genTxBodyContentWithTxIns
  :: CardanoEra era
  -> [(TxIn, BuildTxWith BuildTx (Witness WitCtxTxIn era))]
  -> Gen (TxBodyContent BuildTx era)
genTxBodyContentWithTxIns era txIns = do
  txInsCollateral <- genTxInsCollateral era
  txOuts <- Gen.list (Range.constant 1 10) (CGen.genTxOut era)
  txFee <- genTxFee era
  txValidityRange <- genTxValidityRange era
  txMetadata <- genTxMetadataInEra era
  txAuxScripts <- genTxAuxScripts era
  let txExtraKeyWits = TxExtraKeyWitnessesNone --TODO: Alonzo era: Generate witness key hashes
  txProtocolParams <- BuildTxWith <$> Gen.maybe CGen.genProtocolParameters
  txWithdrawals <- genTxWithdrawals era
  txCertificates <- genTxCertificates era
  txUpdateProposal <- genTxUpdateProposal era
  txMintValue <- genTxMintValue era
  txScriptValidity <- genTxScriptValidity era

  pure $ TxBodyContent
    { Api.txIns
    , Api.txInsCollateral
    , Api.txOuts
    , Api.txFee
    , Api.txValidityRange
    , Api.txMetadata
    , Api.txAuxScripts
    , Api.txExtraKeyWits
    , Api.txProtocolParams
    , Api.txWithdrawals
    , Api.txCertificates
    , Api.txUpdateProposal
    , Api.txMintValue
    , Api.txScriptValidity
    }

genPlutusScriptTxIn :: Gen ((TxIn, Witness WitCtxTxIn AlonzoEra), ScriptHash)
genPlutusScriptTxIn = do
  let plutusScriptVersion = PlutusScriptV1 :: PlutusScriptVersion PlutusScriptV1
  script <- CGen.genPlutusScript plutusScriptVersion
  scriptData <- CGen.genScriptData
  executionUnits <- genExecutionUnits
  let witness =
        ScriptWitness ScriptWitnessForSpending
          (PlutusScriptWitness
           PlutusScriptV1InAlonzo
           PlutusScriptV1
           script
           (ScriptDatumForTxIn scriptData)
           scriptData
           executionUnits)
  txIn <- CGen.genTxIn
  pure ((txIn, witness), hashScript $ PlutusScript PlutusScriptV1 script)


panic :: String -> a
panic = error

-- * Copy-paste

-- | Following is from cardano-node commit
-- 2b1d18c6c7b7142d9eebfec34da48840ed4409b6 (what plutus-apps main
-- depends on) cardano-api/gen/Gen/Cardano/Api/Typed.hs

type TxIns build era = [(TxIn, BuildTxWith build (Witness WitCtxTxIn era))]
-- ^ From /cardano-node/cardano-api/src/Cardano/Api/TxBody.hs

genTxBodyContent :: CardanoEra era -> Gen (TxBodyContent BuildTx era)
genTxBodyContent era = do
  txIns <- map (, BuildTxWith (KeyWitness KeyWitnessForSpending)) <$> Gen.list (Range.constant 1 10) CGen.genTxIn
  txInsCollateral <- genTxInsCollateral era
  txOuts <- Gen.list (Range.constant 1 10) (CGen.genTxOut era)
  txFee <- genTxFee era
  txValidityRange <- genTxValidityRange era
  txMetadata <- genTxMetadataInEra era
  txAuxScripts <- genTxAuxScripts era
  let txExtraKeyWits = TxExtraKeyWitnessesNone --TODO: Alonzo era: Generate witness key hashes
  txProtocolParams <- BuildTxWith <$> Gen.maybe CGen.genProtocolParameters
  txWithdrawals <- genTxWithdrawals era
  txCertificates <- genTxCertificates era
  txUpdateProposal <- genTxUpdateProposal era
  txMintValue <- genTxMintValue era
  txScriptValidity <- genTxScriptValidity era

  pure $ TxBodyContent
    { Api.txIns
    , Api.txInsCollateral
    , Api.txOuts
    , Api.txFee
    , Api.txValidityRange
    , Api.txMetadata
    , Api.txAuxScripts
    , Api.txExtraKeyWits
    , Api.txProtocolParams
    , Api.txWithdrawals
    , Api.txCertificates
    , Api.txUpdateProposal
    , Api.txMintValue
    , Api.txScriptValidity
    }

genTxScriptValidity :: CardanoEra era -> Gen (TxScriptValidity era)
genTxScriptValidity era = case txScriptValiditySupportedInCardanoEra era of
  Nothing      -> pure TxScriptValidityNone
  Just witness -> TxScriptValidity witness <$> genScriptValidity

genScriptValidity :: Gen ScriptValidity
genScriptValidity = Gen.element [ScriptInvalid, ScriptValid]

genTxMintValue :: CardanoEra era -> Gen (TxMintValue BuildTx era)
genTxMintValue era =
  case multiAssetSupportedInEra era of
    Left _ -> pure TxMintNone
    Right supported ->
      Gen.choice
        [ pure TxMintNone
        , TxMintValue supported <$> CGen.genValueForMinting <*> return (BuildTxWith mempty)
        ]

genTxUpdateProposal :: CardanoEra era -> Gen (TxUpdateProposal era)
genTxUpdateProposal era =
  case updateProposalSupportedInEra era of
    Nothing -> pure TxUpdateProposalNone
    Just supported ->
      Gen.choice
        [ pure TxUpdateProposalNone
        , TxUpdateProposal supported <$> CGen.genUpdateProposal
        ]

genTxCertificates :: CardanoEra era -> Gen (TxCertificates BuildTx era)
genTxCertificates era =
  case certificatesSupportedInEra era of
    Nothing -> pure TxCertificatesNone
    Just supported -> do
      certs <- Gen.list (Range.constant 0 3) CGen.genCertificate
      Gen.choice
        [ pure TxCertificatesNone
        , pure (TxCertificates supported certs $ BuildTxWith mempty)
          -- TODO: Generate certificates
        ]

genTxWithdrawals :: CardanoEra era -> Gen (TxWithdrawals BuildTx era)
genTxWithdrawals era =
  case withdrawalsSupportedInEra era of
    Nothing -> pure TxWithdrawalsNone
    Just supported ->
      Gen.choice
        [ pure TxWithdrawalsNone
        , pure (TxWithdrawals supported mempty)
          -- TODO: Generate withdrawals
        ]

genTxAuxScripts :: CardanoEra era -> Gen (TxAuxScripts era)
genTxAuxScripts era =
  case auxScriptsSupportedInEra era of
    Nothing -> pure TxAuxScriptsNone
    Just supported ->
      TxAuxScripts supported <$>
        Gen.list (Range.linear 0 3)
                 (CGen.genScriptInEra era)

genTxMetadataInEra :: CardanoEra era -> Gen (TxMetadataInEra era)
genTxMetadataInEra era =
  case txMetadataSupportedInEra era of
    Nothing -> pure TxMetadataNone
    Just supported ->
      Gen.choice
        [ pure TxMetadataNone
        , TxMetadataInEra supported <$> CGen.genTxMetadata
        ]

genTxValidityRange
  :: CardanoEra era
  -> Gen (TxValidityLowerBound era, TxValidityUpperBound era)
genTxValidityRange era =
  (,)
    <$> genTxValidityLowerBound era
    <*> genTxValidityUpperBound era

-- TODO: Accept a range for generating ttl.
genTxValidityUpperBound :: CardanoEra era -> Gen (TxValidityUpperBound era)
genTxValidityUpperBound era =
  case (validityUpperBoundSupportedInEra era,
       validityNoUpperBoundSupportedInEra era) of
    (Just supported, _) ->
      TxValidityUpperBound supported <$> genTtl

    (Nothing, Just supported) ->
      pure (TxValidityNoUpperBound supported)

    (Nothing, Nothing) ->
      panic "genTxValidityUpperBound: unexpected era support combination"

genTtl :: Gen SlotNo
genTtl = genSlotNo

genSlotNo :: Gen SlotNo
genSlotNo = SlotNo <$> Gen.word64 Range.constantBounded

-- TODO: Accept a range for generating ttl.
genTxValidityLowerBound :: CardanoEra era -> Gen (TxValidityLowerBound era)
genTxValidityLowerBound era =
  case validityLowerBoundSupportedInEra era of
    Nothing        -> pure TxValidityNoLowerBound
    Just supported -> TxValidityLowerBound supported <$> genTtl

genTxFee :: CardanoEra era -> Gen (TxFee era)
genTxFee era =
  case txFeesExplicitInEra era of
    Left  supported -> pure (TxFeeImplicit supported)
    Right supported -> TxFeeExplicit supported <$> CGen.genLovelace

genTxInsCollateral :: CardanoEra era -> Gen (TxInsCollateral era)
genTxInsCollateral era =
    case collateralSupportedInEra era of
      Nothing        -> pure TxInsCollateralNone
      Just supported -> Gen.choice
                          [ pure TxInsCollateralNone
                          , TxInsCollateral supported <$> Gen.list (Range.linear 0 10) CGen.genTxIn
                          ]

genExecutionUnits :: Gen ExecutionUnits
genExecutionUnits = ExecutionUnits <$> Gen.integral (Range.constant 0 1000)
                                   <*> Gen.integral (Range.constant 0 1000)
