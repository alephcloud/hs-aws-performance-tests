{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE DeriveDataTypeable #-}

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
-- * Test Parameters
  testDataPrefix

-- * Exceptions
, TestException(..)
, testThrowT
, toE
, catchET
, fromEitherET
, fromEitherET_

-- * General Utils
, sshow
, tryT
, retryT
, retryT_
, testData
, whenJust

-- * Time Measurement
, time
, timeT
, timeoutT
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
import Data.Time
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Typeable

import System.Exit (ExitCode)
import System.Timeout

-- -------------------------------------------------------------------------- --
-- Static Test parameters
--

-- | This prefix is used for the IDs and names of all entities that are
-- created in the AWS account.
--
testDataPrefix :: IsString a => a
testDataPrefix = "__TEST_AWSHASKELLBINDINGS__"

-- -------------------------------------------------------------------------- --
-- Test Exceptions

data TestException
    = TestException T.Text
    | RetryException Int LE.SomeException
    deriving (Show, Typeable)

instance LE.Exception TestException

testThrowT :: (Monad m) => T.Text -> EitherT LE.SomeException m a
testThrowT = left . LE.toException . TestException

-- | Generalize Exceptions within an 'EitherT' to 'SomeException'
--
toE :: (Monad m, LE.Exception e) => EitherT e m a -> EitherT LE.SomeException m a
toE = fmapLT LE.toException

catchET
    :: (Monad m, LE.Exception e)
    => EitherT LE.SomeException m a
    -> (e -> EitherT LE.SomeException m a)
    -> EitherT LE.SomeException m a
catchET f handler = f `catchT` \e  -> maybe (left e) handler $ LE.fromException e

fromEitherET
    :: (Monad m, LE.Exception e)
    => EitherT LE.SomeException m a
    -> (Maybe e -> m a)
    -> m a
fromEitherET f handler = eitherT (handler . LE.fromException) return f

fromEitherET_
    :: (Monad m, LE.Exception e)
    => EitherT LE.SomeException m a
    -> (Either LE.SomeException e -> m a)
    -> m a
fromEitherET_ f handler = eitherT
    (\e -> handler . maybe (Left e) Right $ LE.fromException e)
    return
    f

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

whenJust :: Monad m => Maybe a -> (a -> m ()) -> m ()
whenJust (Just x) = ($ x)
whenJust Nothing = const $ return ()

-- -------------------------------------------------------------------------- --
-- Time Measurment

getTime :: IO Double
getTime = realToFrac <$> getPOSIXTime

time :: IO a -> IO (NominalDiffTime, a)
time act = do
  start <- getTime
  result <- act
  end <- getTime
  let !delta = end - start
  return (realToFrac delta, result)

timeT :: MonadIO m => m a -> m (NominalDiffTime, a)
timeT act = do
  start <- liftIO getTime
  result <- act
  end <- liftIO getTime
  let !delta = end - start
  return (realToFrac delta, result)

timeoutT
    :: (MonadBaseControl IO m)
    => T.Text           -- ^ label
    -> (T.Text -> b)    -- ^ exception constructor
    -> NominalDiffTime  -- ^ timeout
    -> EitherT b m a    -- ^ action
    -> EitherT b m a
timeoutT label exConstr t a = do
    r <- liftBaseWith $ \runInBase ->
        timeout (round $ t * 1e6) (runInBase a)
    case r of
        Nothing -> left $ exConstr $ label <> " timed out after " <> sshow t
        Just x -> restoreM x

