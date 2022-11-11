{-# LANGUAGE OverloadedStrings #-}

module RollbackRingBuffer where

import Hedgehog (MonadTest, Property, assert, (===))
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testPropertyNamed)

import Marconi.Streaming.ChainSync

tests :: TestTree
tests = testGroup "rollbackRingBuffer"
  [ testPropertyNamed "prop_ringbuffer" "testRollbackRingBuffer" testRollbackRingBuffer
  ]

testRollbackRingBuffer :: Property
testRollbackRingBuffer = do
  undefined
