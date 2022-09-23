{-# LANGUAGE RankNTypes    #-}
{-# LANGUAGE TypeOperators #-}

module Marconi.Streaming.Util where

import Control.Monad.Primitive (PrimState)
import Control.Monad.Trans.Class (lift)
import Data.Vector qualified as V
import Data.Vector.Generic qualified as VG
import Data.Vector.Generic.Mutable qualified as VGM
import Numeric.Natural (Natural)
import Streaming.Prelude qualified as S

-- | Helper type to represent conversion from stream of @a@'s into
-- stream of @b@'s.
type a :-> b = forall r . S.Stream (S.Of a) IO r -> S.Stream (S.Of b) IO r

type Vec = VG.Mutable V.Vector (PrimState IO)

-- | Ring buffer of @n@ elements: blocks until @n@-th element, then
-- yields older elements as new ones are written to the buffer.
ringBuffer :: forall a . Natural -> a :-> a
ringBuffer n source = let
  size = fromIntegral $ toInteger n :: Int
  split = S.splitAt size source
  in do

  -- fill the buffer, don't yield anything
  (vector :: Vec a) S.:> (rest :: S.Stream (S.Of a) IO r) <- lift $
    S.foldM
      (\(i, v) a -> do
          VGM.unsafeWrite v i a
          pure (i + 1, v))
      (do vector <- VGM.new size
          pure (0, vector))
      (\(_, v) -> pure v)
      split

  -- fill & yield from the buffer
  let zippedWithIndex = S.zip (S.cycle (S.each [0 .. (size - 1)])) rest
  flip S.mapM zippedWithIndex $ \(i, a) -> do
    VGM.exchange vector i a
