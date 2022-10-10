{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE GADTs             #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module Marconi.Indexers.StakePoolDelegation where

import Control.Concurrent qualified as IO
import Control.Monad (void)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (forM_)
import Data.Function ((&))
import Data.Functor (($>))
import Data.Map qualified as M
import Data.Maybe qualified as P
import Database.SQLite.Simple qualified as SQL
import Streaming.Prelude qualified as S
import System.IO

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as Shelley
import Cardano.Ledger.BaseTypes qualified as L
import Cardano.Ledger.Coin qualified as L
import Cardano.Ledger.Crypto qualified as LC
import Cardano.Ledger.Era qualified as LE
import Cardano.Ledger.Keys qualified as LK
import Cardano.Ledger.PoolDistr qualified as Pd
import Cardano.Ledger.Shelley.API.Wallet qualified as Shelley
import Cardano.Ledger.Shelley.EpochBoundary qualified as EB
import Cardano.Ledger.Shelley.Genesis qualified as Shelley
import Cardano.Ledger.Shelley.LedgerState qualified as SL
import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))
import Ledger.Tx.CardanoAPI (withIsCardanoEra)
import Ouroboros.Consensus.Shelley.Eras qualified as O
import Ouroboros.Consensus.Shelley.Ledger qualified as O

import Marconi.Streaming.ChainSync (chainEventSource, rollbackRingBuffer)


data Event = Event C.SlotNo [(C.StakeCredential, Shelley.PoolId)]

toEvent :: ChainSyncEvent (C.BlockInMode C.CardanoMode) -> ChainSyncEvent Event
toEvent = \case
  RollForward (C.BlockInMode (C.Block (C.BlockHeader slotNo _ _) txs :: C.Block era) era) ct -> do
    withIsCardanoEra era (RollForward event ct)
    where
      event = Event slotNo (txs >>= txStakeDelegations)
      txStakeDelegations (C.Tx body _witnesses) = case body of
        C.TxBody txBodyContent -> let
          certificates = case C.txCertificates txBodyContent :: C.TxCertificates Shelley.ViewTx era of
            C.TxCertificates _ certs' _ -> certs'
            _                           -> []
          in P.mapMaybe maybeDelegationCertificate certificates

      maybeDelegationCertificate :: C.Certificate -> Maybe (C.StakeCredential, Shelley.PoolId)
      maybeDelegationCertificate = \case
        Shelley.StakeAddressDelegationCertificate stakeCredential poolId -> Just (stakeCredential, poolId)
        _                                                                -> Nothing

        -- using: data TxBody era where
        -- Shelley.ShelleyTxBody _shelleyBasedEra _ledgerTxBody _ledgerScripts _scriptData _maybeAuxData _scriptValidity -> undefined
        -- _ -> [] -- Byron transactions can't delegate staking

  RollBackward cp ct -> RollBackward cp ct

sqlite
  :: FilePath
  -> S.Stream (S.Of Event) IO r
  -> S.Stream (S.Of Event) IO r
sqlite db source = do
  c <- lift $ do
    c' <- SQL.open db
    SQL.execute_ c'
      "CREATE TABLE IF NOT EXISTS stakepool_delegation (poolId TEXT NOT NULL, lovelace INT NOT NULL, epochId TEXT NOT NULL)"
    pure c'

  let loop source' = lift (S.next source') >>= \case
        Left r -> pure r
        Right (event, source'') -> let
          rows = undefined :: [(Int, Int)]
        --   ScriptTxUpdate txScriptAddrs _slotNo = scriptTxUpdate
        --   rows = do
        --     (txCbor', scriptAddrs) <- txScriptAddrs
        --     scriptAddr <- scriptAddrs
        --     pure $ ScriptTxRow scriptAddr txCbor'
          in do
          lift $ forM_ rows $ SQL.execute c
            "INSERT INTO stakepool_delegation (poolId, lovelace, epochId) VALUES (?, ?)"
          S.yield event
          loop source''

  loop source

indexer :: FilePath -> C.NetworkId -> Shelley.ChainPoint -> FilePath -> IO ()
indexer socket networkId chainPoint db = chainEventSource  socket networkId chainPoint
  & S.map toEvent
  & rollbackRingBuffer 2160
  & sqlite db
  & S.effects

-- * FoldBlocks

ledgerStates :: (FilePath, FilePath) -> S.Stream (S.Of C.LedgerState) IO a
ledgerStates (socketPath, nodeConfigPath) = do
  chan <- lift IO.newChan
  lift $ IO.forkIO $ either (const $ pure ()) (const $ pure ()) =<< runExceptT (C.foldBlocks nodeConfigPath socketPath C.QuickValidation () $
    \_env ledgerState _ledgerEvents _blockInCardanoMode _ -> IO.writeChan chan ledgerState)
  S.repeatM $ IO.readChan chan

type A = Int

hot :: IO ()
hot = let
  (socketPath, nodeConfig) = mainnet
  go :: C.Env -> C.LedgerState -> [C.LedgerEvent] -> C.BlockInMode C.CardanoMode -> A -> IO A
  go _env ledgerState _ledgerEvents _blockInCardanoMode a = let
    C.LedgerState (_) = ledgerState
    res = getPoolSizes ledgerState
    in putStrLn (show a <> ": " <> show res) $> a + 1

  foldBlocks' = C.foldBlocks nodeConfig socketPath C.QuickValidation 0 go

  in either (print . C.renderFoldBlocksError) print =<< runExceptT foldBlocks'




type Result era = M.Map (LK.KeyHash 'LK.StakePool era) (Pd.IndividualPoolStake era)
-- type Result era v = M.Map (LK.KeyHash 'LK.StakePool era) (Pd.IndividualPoolStake era)

type Result' = Int

getPoolSizes :: C.LedgerState -> Maybe Result'
getPoolSizes ledgerState = case ledgerState of
  C.LedgerStateByron _    -> Nothing
  C.LedgerStateShelley st -> fromState st
  C.LedgerStateAllegra st -> fromState st
  C.LedgerStateMary st    -> fromState st
  C.LedgerStateAlonzo st  -> fromState st
  where
    fromState :: forall era . O.LedgerState (O.ShelleyBlock era) -> Maybe Result'
    fromState st = Just r
      where
        nes = O.shelleyLedgerState st
        m = Pd.unPoolDistr $ SL.nesPd nes
        r = length $ M.elems $ fmap Pd.individualPoolStake m

        _not = fmap toAbsolute m :: M.Map (LK.KeyHash 'LK.StakePool (LE.Crypto era)) L.Coin

        toAbsolute :: Pd.IndividualPoolStake (LE.Crypto era) -> L.Coin
        toAbsolute = undefined

        totalStake :: L.Coin
        totalStake = Shelley.getTotalStake globals nes

        globals = undefined :: L.Globals

-- * Tmp

preview, preprod, mainnet :: (FilePath, FilePath)
preview = ( "/home/markus/preview/socket/node.socket"
          , "/home/markus/preview/config/config.json" )
preprod = ( "/home/markus/preprod/socket/node.socket"
          , "/home/markus/preprod/config/config.json" )
mainnet = ( "/home/markus/cardano/socket/node.socket"
          , "/home/markus/cardano/config/config.json" )

-- constructGlobals
--   :: Shelley.ShelleyGenesis O.StandardShelley
--   -> EpochInfo (Either Text)
--   -> Shelley.ProtocolParameters
--   -> L.Globals
constructGlobals sGen eInfo pParams =
  let majorPParamsVer = fst $ Shelley.protocolParamProtocolVersion pParams
  in Shelley.mkShelleyGlobals sGen eInfo majorPParamsVer

-- getTotalStake :: Globals -> NewEpochState era -> Coin
-- file:/home/iog/src/cardano-ledger/eras/shelley/impl/src/Cardano/Ledger/Shelley/API/Wallet.hs::231
-- totalStake :: Shelley.RewardParams -> L.Coin

-- currentSnapshot :: SL.NewEpochState era -> EB.SnapShot (O.EraCrypto era)
-- currentSnapshot ss =
--   SL.incrementalStakeDistr incrementalStake dstate pstate
--   where
--     ledgerState = esLState $ nesEs ss
--     incrementalStake = _stakeDistro $ lsUTxOState ledgerState
--     dstate = dpsDState $ lsDPState ledgerState
--     pstate = dpsPState $ lsDPState ledgerState
