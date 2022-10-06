{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module Marconi.Indexers.StakePoolDelegation where

import Control.Monad (void)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT)
import Data.Foldable (forM_)
import Data.Function ((&))
import Data.Maybe qualified as P
import Database.SQLite.Simple qualified as SQL
import Streaming.Prelude qualified as S
import System.IO

import Cardano.Api qualified as C
import Cardano.Api.Shelley qualified as Shelley
import Cardano.Streaming (ChainSyncEvent (RollBackward, RollForward))
import Ledger.Tx.CardanoAPI (withIsCardanoEra)

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


type A = Int

hot :: IO ()
hot = let
  preview = ( "/home/markus/preview/socket/node.socket" :: FilePath
            , "/home/markus/preview/config/config.json" :: FilePath )

  preprod = ( "/home/markus/preprod/socket/node.socket" :: FilePath
            , "/home/markus/preprod/config/config.json" :: FilePath )

  mainnet = ( "/home/markus/cardano/socket/node.socket" :: FilePath
            , "/home/markus/cardano/config/config.json" :: FilePath )

  (socketPath, nodeConfig) = mainnet

  go :: C.Env -> C.LedgerState -> [C.LedgerEvent] -> C.BlockInMode C.CardanoMode -> A -> IO A
  go _env _ledgerState _ledgerEvents _blockInCardanoMode a = do
    print a
    pure $ a + 1

  _this = C.foldBlocks nodeConfig socketPath C.QuickValidation 0 go

  in do
  either (print . C.renderFoldBlocksError) print =<< runExceptT _this
