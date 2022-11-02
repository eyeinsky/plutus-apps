{-# LANGUAGE NamedFieldPuns #-}

module Main where

import Cardano.Api (ChainPoint, NetworkId (Mainnet, Testnet), NetworkMagic (NetworkMagic))
import Marconi.CLI (chainPointParser)
import Options.Applicative (Mod, OptionFields, Parser, auto, execParser, flag', help, helper, info, long, metavar,
                            option, strOption, (<**>), (<|>))

data Options = Options
  { optionsSocketPath :: String
  , optionsNetworkId  :: NetworkId
  , optionsChainPoint :: ChainPoint
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

networkIdParser :: Parser NetworkId
networkIdParser = pMainnet <|> pTestnet
  where
    pMainnet :: Parser NetworkId
    pMainnet = flag' Mainnet ( long "mainnet" <> help "Use the mainnet magic id.")

    pTestnet :: Parser NetworkId
    pTestnet = Testnet . NetworkMagic <$> option auto
        ( long "testnet-magic"
          <> metavar "NATURAL"
          <> help "Specify a testnet magic id."
        )

main :: IO ()
main = do
  Options { optionsSocketPath
          , optionsNetworkId
          , optionsChainPoint
          } <- parseOptions

  putStrLn "hello"
