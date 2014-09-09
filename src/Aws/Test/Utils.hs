{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE RankNTypes #-}

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

-- * Connection Statistics
, Stat(..)
, successStat
, failStat
, logSuccess
, logFailure
, pruneHttpError
, printStat
, writeStatFiles
, writeSample
, readSample
#ifdef WITH_CHART
, writeStatChart
#endif
) where

import Data.Default
import Control.Applicative
import Control.Concurrent (threadDelay)
import qualified Control.Exception.Lifted as LE
import Control.Error hiding (syncIO)
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Control

import qualified Data.CaseInsensitive as CI
import qualified Data.DList as D
import Data.Dynamic (Dynamic)
import Data.IORef
import qualified Data.List as L
import Data.Monoid
import qualified Data.Set as S
import Data.String
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Read as T
import Data.Time
import Data.Time.Clock.POSIX (getPOSIXTime)
import qualified Data.Vector.Unboxed as V
import Data.Typeable

import qualified Network.HTTP.Client as HTTP

import qualified Statistics.Function as ST
import qualified Statistics.Sample as ST

import System.Exit (ExitCode)
import System.IO
import System.Timeout

import Text.Printf

#ifdef WITH_CHART
-- Used for plotting
import Control.Arrow ((***))

import Data.Colour
import Data.Colour.Names
import Control.Lens hiding (act, (.=))

import Graphics.Rendering.Chart hiding (label)
import Graphics.Rendering.Chart.Backend.Cairo

import qualified Statistics.Sample.KernelDensity as ST
#endif


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

retryT :: (LE.Exception e, MonadIO m) => Int -> EitherT e m a -> EitherT TestException m a
retryT n f = snd <$> retryT_ n f

retryT_ :: (LE.Exception e, MonadIO m) => Int -> EitherT e m a -> EitherT TestException m (Int, a)
retryT_ n f = go 1
  where
    go x
        | x >= n = fmapLT (RetryException x . LE.toException) ((x,) <$> f)
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

-- -------------------------------------------------------------------------- --
-- Connection Statistics

data Stat = Stat
    { statFailure :: !Int
    , statSuccess :: !Int
    , statFailureLatency :: !(D.DList Double) -- ^ latency in milliseconds
    , statSuccessLatency :: !(D.DList Double) -- ^ latency in milliseconds
    , statFailureMessages :: !(S.Set T.Text)
    }
    deriving (Show, Eq, Ord, Typeable)

instance Monoid Stat where
    mempty = Stat 0 0 mempty mempty mempty
    (Stat a0 a1 a2 a3 a4) `mappend` (Stat b0 b1 b2 b3 b4) = Stat
        (a0 + b0)
        (a1 + b1)
        (a2 <> b2)
        (a3 <> b3)
        (a4 <> b4)

successStat
    :: Double -- milliseconds
    -> Stat
successStat l = Stat 0 1 mempty (D.singleton l) mempty

failStat
    :: Double -- millisconds
    -> T.Text -- failure message
    -> Stat
failStat l e = Stat 1 0 (D.singleton l) mempty (S.singleton e)

logSuccess
    :: IORef Stat
    -> NominalDiffTime
    -> IO ()
logSuccess ref t = atomicModifyIORef' ref $ \stat ->
    (stat <> successStat (realToFrac t * 1000), ())

logFailure
    :: IORef Stat
    -> NominalDiffTime
    -> T.Text
    -> IO ()
logFailure ref t e = atomicModifyIORef' ref $ \stat ->
    (stat <> failStat (realToFrac t * 1000) e, ())

-- | Prune HTTP error such that similar exceptions become equal and are
-- logged only once.
--
pruneHttpError :: HTTP.HttpException -> HTTP.HttpException
pruneHttpError (HTTP.StatusCodeException s h _) = HTTP.StatusCodeException s (deleteDateHeader h) def
pruneHttpError (HTTP.TooManyRedirects _ ) = HTTP.TooManyRedirects []
pruneHttpError e = e

deleteDateHeader :: (Eq a, IsString a, CI.FoldCase a) => [(CI.CI a, b)] -> [(CI.CI a, b)]
deleteDateHeader = L.filter ((/= "date") . fst)

toSample
    :: D.DList Double
    -> ST.Sample
toSample = V.fromList . D.toList

printStat
    :: T.Text
    -> NominalDiffTime
    -> Stat
    -> IO ()
printStat testName totalTime Stat{..} = do

    -- Overview
    printf "Test \"%v\" completed %v requests (%v successes, %v failures) in %.2fs\n\n"
        (T.unpack testName)
        (statSuccess + statFailure)
        statSuccess
        statFailure
        (realToFrac totalTime :: Double)

    -- Successes
    let (succMin, succMax) = ST.minMax succSample
        succMean = ST.mean succSample
        succStdDev = ST.stdDev succSample
    printf "Success latencies\n"
    printf "    min: %.2fms, max %.2fms\n" succMin succMax
    printf "    mean: %.2fms, standard deviation: %.2fms\n\n" succMean succStdDev

    -- Failures
    unless (statFailure == 0) $ do
        let (failMin, failMax) = ST.minMax failSample
            failMean = ST.mean failSample
            failStdDev = ST.stdDev failSample
        printf "Failure latencies\n"
        printf "    min: %.2fms, max %.2fms\n" failMin failMax
        printf "    mean: %.2fms, standard deviation %.2fms\n\n" failMean failStdDev

        -- Failure Messages
        printf "Failure Messages:\n"
        forM_ (S.toList statFailureMessages) $ \e ->
            T.putStrLn $ "    " <> sshow e
        printf "\n"
  where
    succSample = toSample statSuccessLatency
    failSample = toSample statFailureLatency

writeStatFiles
    :: String -- ^ file name prefix
    -> T.Text -- ^ test name
    -> Stat -- ^ results
    -> IO ()
writeStatFiles prefix testName Stat{..} = do
    writeSample (prefix <> "-" <> T.unpack testName <> "-success.txt") (toSample statSuccessLatency)
    writeSample (prefix <> "-" <> T.unpack testName <> "-failure.txt") (toSample statFailureLatency)

#ifdef WITH_CHART
-- -------------------------------------------------------------------------- --
-- Plotting of Connection Statistics

chart
    :: T.Text -- ^ title of the chart
    -> [(String, Colour Double, [(LogValue, Double)])] -- ^ title color and data for each plot
    -> Renderable ()
chart chartTitle dats = toRenderable layout
  where
    pl (title, color, results) = def
        & plot_points_title .~ title
        & plot_points_style . point_color .~ opaque color
        & plot_points_style . point_radius .~ 1
        & plot_points_values .~ results

    layout = def
        & layout_title .~ T.unpack chartTitle
        & layout_background .~ solidFillStyle (opaque white)
        & layout_left_axis_visibility . axis_show_ticks .~ False
#if !MIN_VERSION_Chart(1,3,0)
        & setLayoutForeground (opaque black)
#endif
        & layout_plots .~ [ toPlot (pl d) | d <- dats ]

densityChart
    :: V.Vector Double
    -> V.Vector Double
    -> Renderable ()
densityChart successes failures = chart "Density" $
    if V.null successes then [] else [("success", blue, succDat)]
    <>
    if V.null failures then [] else [("failures", red, failDat)]
  where
    succDat,failDat :: [(LogValue, Double)]
    succDat = uncurry zip . (map LogValue . V.toList *** map (* 2048) . V.toList) $ ST.kde 2048 successes
    failDat = uncurry zip . (map LogValue . V.toList *** map (* 2048) . V.toList) $ ST.kde 2048 failures

writeStatChart
    :: String -- ^ file name prefix
    -> T.Text -- ^ test name
    -> Stat -- ^ results
    -> IO ()
writeStatChart prefix testName Stat{..} = void $
#if MIN_VERSION_Chart_cairo(1,3,0)
    renderableToFile opts path render
#else
    renderableToFile opts render path
#endif
  where
    path = prefix <> "-" <> T.unpack testName <> "-density.pdf"
    opts = FileOptions (800,600) PDF
    render = densityChart (toSample statSuccessLatency) (toSample statFailureLatency)

#endif

-- -------------------------------------------------------------------------- --
-- Serialize Connection Statistics

writeSample
    :: FilePath
    -> ST.Sample
    -> IO ()
writeSample file sample = withFile file WriteMode $ \h ->
    V.forM_ sample $ T.hPutStrLn h . sshow

readSample
    :: FilePath
    -> IO ST.Sample
readSample file = withFile file ReadMode $ fmap V.fromList . go
  where
    go h = hIsEOF h >>= \x -> if x
        then return []
        else do
            r <- either error fst . T.double <$> T.hGetLine h
            (:) r <$> go h

