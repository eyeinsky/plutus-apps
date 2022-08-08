module MarconiSpec where

-- Regarding property testing, here's a starting point.

-- You can test the txScripts function as follows. Generate a random
-- number of plutus scripts (see `genPlutusScript` from
-- Gen.Cardano.Api.Typed), then generate an TxIns which spends funds
-- from these scripts, then generate a transaction with this TxIns
-- (based on `genTxBodyContent`), run your txScripts on this generated
-- tx, and verify that the initial generated plutus scripts are part
-- of the function's output.

import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Tasty (TestTree, defaultMain, testGroup)
-- import Test.Tasty.Hedgehog (testPropertyNamed)
import Test.Tasty.Hedgehog (testProperty)

tests :: TestTree
tests = testGroup "marconi"
  [ testProperty "prop_script_hashes_in_tx_match" undefined ]
