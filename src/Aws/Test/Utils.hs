{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}

-- |
-- Module: Aws.Test.Utils
-- Copyright: Copyright © 2014 AlephCloud Systems, Inc.
-- License: MIT
-- Maintainer: Lars Kuhtz <lars@alephcloud.com>
-- Stability: experimental
--
-- Utils for testing the Haskell bindings for Amazon Web Services (AWS)
--
module Aws.Test.Utils
(
-- * Parameters
  testDataPrefix

-- * General Utils
, sshow
, tryT
, retryT
, retryT_
, testData
) where

import Control.Applicative
import Control.Concurrent (threadDelay)
import qualified Control.Exception.Lifted as LE
import Control.Error hiding (syncIO)
import Control.Monad.IO.Class
import Control.Monad.Trans.Control

import Data.Dynamic (Dynamic)
import Data.Monoid
import Data.String
import qualified Data.Text as T

import System.Exit (ExitCode)

-- -------------------------------------------------------------------------- --
-- Static Test parameters
--

-- | This prefix is used for the IDs and names of all entities that are
-- created in the AWS account.
--
testDataPrefix :: IsString a => a
testDataPrefix = "__TEST_AWSHASKELLBINDINGS__"

-- -------------------------------------------------------------------------- --
-- General Utils

-- | Catches all exceptions except for asynchronous exceptions found in base.
--
tryT :: MonadBaseControl IO m => m a -> EitherT T.Text m a
tryT = fmapLT (T.pack . show) . syncIO

-- | Lifted Version of 'syncIO' form "Control.Error.Util".
--
syncIO :: MonadBaseControl IO m => m a -> EitherT LE.SomeException m a
syncIO a = EitherT $ LE.catches (Right <$> a)
    [ LE.Handler $ \e -> LE.throw (e :: LE.ArithException)
    , LE.Handler $ \e -> LE.throw (e :: LE.ArrayException)
    , LE.Handler $ \e -> LE.throw (e :: LE.AssertionFailed)
    , LE.Handler $ \e -> LE.throw (e :: LE.AsyncException)
    , LE.Handler $ \e -> LE.throw (e :: LE.BlockedIndefinitelyOnMVar)
    , LE.Handler $ \e -> LE.throw (e :: LE.BlockedIndefinitelyOnSTM)
    , LE.Handler $ \e -> LE.throw (e :: LE.Deadlock)
    , LE.Handler $ \e -> LE.throw (e ::    Dynamic)
    , LE.Handler $ \e -> LE.throw (e :: LE.ErrorCall)
    , LE.Handler $ \e -> LE.throw (e ::    ExitCode)
    , LE.Handler $ \e -> LE.throw (e :: LE.NestedAtomically)
    , LE.Handler $ \e -> LE.throw (e :: LE.NoMethodError)
    , LE.Handler $ \e -> LE.throw (e :: LE.NonTermination)
    , LE.Handler $ \e -> LE.throw (e :: LE.PatternMatchFail)
    , LE.Handler $ \e -> LE.throw (e :: LE.RecConError)
    , LE.Handler $ \e -> LE.throw (e :: LE.RecSelError)
    , LE.Handler $ \e -> LE.throw (e :: LE.RecUpdError)
    , LE.Handler $ return . Left
    ]

testData :: (IsString a, Monoid a) => a -> a
testData a = testDataPrefix <> a

retryT :: MonadIO m => Int -> EitherT T.Text m a -> EitherT T.Text m a
retryT n f = snd <$> retryT_ n f

retryT_ :: MonadIO m => Int -> EitherT T.Text m a -> EitherT T.Text m (Int, a)
retryT_ n f = go 1
  where
    go x
        | x >= n = fmapLT (\e -> "error after " <> sshow x <> " retries: " <> e) ((x,) <$> f)
        | otherwise = ((x,) <$> f) `catchT` \_ -> do
            liftIO $ threadDelay (1000000 * min 60 (2^(x-1)))
            go (succ x)

sshow :: (Show a, IsString b) => a -> b
sshow = fromString . show

