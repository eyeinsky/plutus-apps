{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Hedgehog (Gen, Property, assert, forAll, property)
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Hedgehog (testPropertyNamed)


main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "Evernode"
  [ ]
