{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}

module Main where

import Control.Arrow
import Control.Concurrent.Async
import Control.Applicative
import Control.Monad

import Data.Monoid
import qualified Data.Foldable as F
import qualified Data.List as L
import qualified Data.DList as D
import qualified Data.Sequence as Seq
import qualified Data.Sequences as S
import qualified Data.MonoTraversable as S
import qualified Data.Set as Set
import qualified Data.Vector.Fusion.Stream as Stream
import qualified Data.Vector.Generic as GV
import qualified Data.Vector.Unboxed as V
import Data.Vector.Instances ()
import qualified Data.Text as T
import Data.Proxy
import Data.Typeable

import Criterion
import Criterion.Main

-- -------------------------------------------------------------------------- --
-- Stat

data Stat a = Stat
    { statFailure :: !Int
    , statSuccess :: !Int
    , statFailureLatency :: !a -- ^ latency in milliseconds
    , statSuccessLatency :: !a -- ^ latency in milliseconds
    , statFailureMessages :: !(Set.Set T.Text)
    }
    deriving (Show, Eq, Ord, Typeable)

instance Monoid a => Monoid (Stat a) where
    mempty = Stat 0 0 mempty mempty mempty
    (Stat a0 a1 a2 a3 a4) `mappend` (Stat b0 b1 b2 b3 b4) = Stat
        (a0 + b0)
        (a1 + b1)
        (a2 <> b2)
        (a3 <> b3)
        (a4 <> b4)
    {-# INLINE mempty #-}
    {-# INLINE mappend #-}

successStat
    :: (Monoid a, S.MonoPointed a, S.Element a ~ Double)
    => Double
    -> Stat a
successStat l = Stat 0 1 mempty (S.opoint l) mempty
{-# INLINE successStat #-}

-- -------------------------------------------------------------------------- --
-- Efficient conversion to Vector

-- -------------------------------------------------------------------------- --
-- DList with Length

newtype DListS a = DListS ([a] -> Int -> ([a], Int))

type instance S.Element (DListS a) = a

instance Functor DListS where
    fmap f (DListS a) = DListS $ \x y -> (fmap f l ++ x, n + y)
      where
        (l,n) = a [] 0
    {-# INLINE fmap #-}

instance S.MonoPointed (DListS a) where
    opoint = pure
    {-# INLINE opoint #-}

instance Monoid (DListS a) where
    mempty = DListS $ \(!x) (!y) -> (x, y)
    (DListS a0) `mappend` (DListS a1) = DListS $ \(!x) (!y) -> uncurry a0 $! a1 x y
    {-# INLINE mappend #-}
    {-# INLINE mempty #-}

instance Applicative DListS where
    pure a = DListS $ \x y -> (a : x, 1 + y)
    (DListS a0) <*> (DListS a1) = DListS $ \x y ->
        ((a0l <*> a1l) ++ x, b0n * b1n + y)
      where
        (a0l, b0n) = a0 [] 0
        (a1l, b1n) = a1 [] 0
    {-# INLINE pure #-}
    {-# INLINE (<*>) #-}

instance F.Foldable DListS where
    foldr f e (DListS a) = L.foldr f e . fst $ a [] 0
    {-# INLINE foldr #-}

-- -------------------------------------------------------------------------- --
-- Stream

type instance S.Element (Stream.Stream a) = a

instance S.MonoPointed (Stream.Stream a) where
    opoint = Stream.singleton
    {-# INLINE opoint #-}

instance Monoid (Stream.Stream a) where
    mempty = Stream.empty
    mappend = (Stream.++)
    {-# INLINE mappend #-}
    {-# INLINE mempty #-}

-- -------------------------------------------------------------------------- --
-- Benchmarks

main :: IO ()
main = defaultMain
    [ genBench "DList" (V.fromList . D.toList)
    , genBench "Seq" (V.fromList . F.toList :: Seq.Seq Double -> V.Vector Double)
    , genBench "DListS" (\(DListS a) -> uncurry (flip V.fromListN) (a [] 0))
    -- , genBench "Stream" GV.unstream
    -- , genBench "Vector" (Proxy :: Proxy (V.Vector Double)) id
    -- , genBench "List" V.fromList
    ]
  where
    n :: Int
    n = 10000

    t :: Int
    t = 100

    genBench
        :: (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => String
        -> (a -> V.Vector Double)
        -> Benchmark
    genBench name conv = bgroup name
            [ bench "IO" $ whnfIO (sBenchIO conv n)
            , bench "foldr" $ whnf (sBenchFoldr conv) n
            , bench "foldl'" $ whnf (sBenchFoldl conv) n
            --
            , bench "Stat IO" $ whnfIO (statBenchIO conv n)
            , bench "Stat foldr" $ whnf (statBenchFoldr conv) n
            , bench "Stat foldl'" $ whnf (statBenchFoldl conv) n
            --
            , bench "IO Concurrent" $ whnfIO (cBenchIO conv n)
            , bench "foldr Concurrent" $ whnfIO (cBenchFoldr conv n)
            , bench "foldl Concurrent" $ whnfIO (cBenchFoldl conv n)
            ]

    -- atomic steps
    step
        :: (Monoid a, S.MonoPointed a, S.Element a ~ Double)
        => a
        -> Int
        -> a
    step s i = s <> S.opoint (realToFrac i)
    {-# INLINE step #-}

    stepM
        :: (Monad m, Monoid a, S.MonoPointed a, S.Element a ~ Double)
        => a
        -> Int
        -> m a
    stepM a b = return $ step a b
    {-# INLINE stepM #-}

    -- plain
    sBenchIO
        :: (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> IO Double
    sBenchIO conv n = V.sum . conv <$> foldM stepM mempty [0..n]

    sBenchFoldl
        :: (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> Double
    sBenchFoldl conv n = V.sum . conv $ L.foldl' step mempty [0..n]

    sBenchFoldr
        :: (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> Double
    sBenchFoldr conv n = V.sum . conv $ L.foldr (flip step) mempty [0..n]

    -- concurrent
    cBenchIO
        :: (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> IO Double
    cBenchIO conv n = V.sum . conv . mconcat <$>
        mapConcurrently (\t -> foldM stepM mempty $ map (+t) [0..n]) [1..t]

    cBenchFoldl
        :: (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> IO Double
    cBenchFoldl conv n = V.sum . conv . mconcat <$>
        mapConcurrently (\t -> return $! L.foldl' step mempty $ map (+t) [0..n]) [1..t]

    cBenchFoldr
        :: forall a . (S.MonoPointed a, Monoid a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> IO Double
    cBenchFoldr conv n = V.sum . conv . mconcat <$>
        mapConcurrently (\t -> return $! L.foldr (flip step . (+t)) mempty [0..n]) [1..t]

    -- Stat
    statBenchIO
        :: (Monoid a, S.MonoPointed a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> IO Double
    statBenchIO conv n = V.sum . conv . statSuccessLatency <$> foldM run mempty [0..n]
      where
        run s i = return $ s <> successStat (realToFrac i)

    statBenchFoldl
        :: (Monoid a, S.MonoPointed a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> Double
    statBenchFoldl conv n = V.sum . conv . statSuccessLatency $ L.foldl' run mempty [0..n]
      where
        run s i = s <> successStat (realToFrac i)

    statBenchFoldr
        :: (Monoid a, S.MonoPointed a, S.Element a ~ Double)
        => (a -> V.Vector Double)
        -> Int
        -> Double
    statBenchFoldr conv n = V.sum . conv . statSuccessLatency $ L.foldr run mempty [0..n]
      where
        run i s = s <> successStat (realToFrac i)

