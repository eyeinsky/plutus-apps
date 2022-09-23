module Streaming where

import Hedgehog (MonadTest, Property, assert, (===))
import Hedgehog qualified as H
import Hedgehog.Extras.Stock.IO.Network.Sprocket qualified as IO
import Hedgehog.Extras.Test qualified as HE
import Hedgehog.Extras.Test.Base qualified as H
import Test.Tasty (TestTree, testGroup)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests = testGroup "Streaming"
  [ testProperty "prop_ring_buffer" testRingBuffer
--  , testProperty "prop_rollback_ring_buffer" testRollbackRingBuffer
  ]

testRingBuffer = undefined

testRollbackRingBuffer = undefined
