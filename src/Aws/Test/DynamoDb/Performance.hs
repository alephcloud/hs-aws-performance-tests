{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE CPP #-}

-- |
-- Module: Main
-- Copyright: Copyright © 2014 AlephCloud Systems, Inc.
-- License: MIT
-- Maintainer: Lars Kuhtz <lars@alephcloud.com>
-- Stability: experimental
--
-- Performance tests for the Haskell bindings for Amazon DynamoDb
--
module Main
( main
) where

import Aws
import qualified Aws.DynamoDb as DY

import Aws.Test.Utils
import Aws.Test.DynamoDb.Utils

import Configuration.Utils

import Control.Concurrent.Async
import Control.Error
import Control.Exception
import Control.Lens hiding (act, (.=))
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Resource

import qualified Data.ByteString.Char8 as B8
import qualified Data.DList as D
import qualified Data.List as L
import qualified Data.Map as M
import Data.Monoid
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Lens as T
import qualified Data.Text.IO as T
import qualified Data.Text.Read as T
import Data.Typeable
import qualified Data.Vector.Unboxed as V

import qualified Network.HTTP.Client as HTTP

import PkgInfo

import qualified Statistics.Function as ST
import qualified Statistics.Sample as ST

import System.IO

import Text.Printf

#ifdef WITH_CHART
-- Used for plotting
import Control.Arrow ((***))

import Data.Default
import Data.Colour
import Data.Colour.Names

import Graphics.Rendering.Chart hiding (label)
import Graphics.Rendering.Chart.Backend.Cairo

import qualified Statistics.Sample.KernelDensity as ST
#endif

-- -------------------------------------------------------------------------- --
-- Statistics

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

successStat :: Double -> Stat
successStat l = Stat 0 1 mempty (D.singleton l) mempty

failStat :: Double -> T.Text -> Stat
failStat l e = Stat 1 0 (D.singleton l) mempty (S.singleton e)

toSample
    :: D.DList Double
    -> ST.Sample
toSample = V.fromList . D.toList

printResult
    :: T.Text
    -> NominalDiffTime
    -> Stat
    -> IO ()
printResult testName totalTime Stat{..} = do

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

writeLatencyData
    :: String -- ^ file name prefix
    -> T.Text -- ^ test name
    -> Stat -- ^ results
    -> IO ()
writeLatencyData prefix testName Stat{..} = do
    writeSample (prefix <> "-" <> T.unpack testName <> "-success.txt") (toSample statSuccessLatency)
    writeSample (prefix <> "-" <> T.unpack testName <> "-failure.txt") (toSample statFailureLatency)

#ifdef WITH_CHART
-- -------------------------------------------------------------------------- --
-- Plotting

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

writeChart
    :: String -- ^ file name prefix
    -> T.Text -- ^ test name
    -> Stat -- ^ results
    -> IO ()
writeChart prefix testName Stat{..} = void $
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
-- Serialize latencies

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

-- -------------------------------------------------------------------------- --
-- Running Tests

testItems :: T.Text -> Int -> [DY.Item]
testItems prefix n = map (\i -> M.singleton "Id" (DY.DString $ prefix <> "-" <> sshow i)) [0..n-1]

testQueries :: T.Text -> Int -> [DY.PrimaryKey]
testQueries prefix n = map (\i -> DY.hk "Id" (DY.DString $ prefix <> "-" <> sshow i)) [0..n-1]

runThread
    :: (Transaction r x, ServiceConfiguration r ~ DY.DdbConfiguration)
    => Configuration
    -> DY.DdbConfiguration NormalQuery
    -> HTTP.Manager
    -> [r]
    -> IO Stat
runThread cfg dyCfg manager = flip foldM mempty $ \stat req -> do
    (t,response) <- time . runResourceT $ aws cfg dyCfg manager req
    case responseResult response of
        Right _ -> return $ stat <> successStat (realToFrac t * 1000)
        Left e -> return $ stat <> failStat (realToFrac t * 1000) (sshow e)

-- | Use a single Manager for all threads
--
runTestGlobalManager
    :: (Transaction r x, ServiceConfiguration r ~ DY.DdbConfiguration)
    => T.Text -- ^ test name
    -> TestParams -- ^ test parameters
    -> (Int -> [r]) -- ^ requests per thread
    -> IO ()
runTestGlobalManager testName TestParams{..} mkRequests = do
    T.putStrLn $ "Start test \"" <> testName <> "\""
    cfg <- baseConfiguration
    (t, stats) <- HTTP.withManager managerSettings $ \manager ->
        time $ mapConcurrently
            (runThread cfg dyCfg manager)
            (map mkRequests [0.. _paramThreadCount - 1])

    -- report results
    let stat = mconcat stats
    printResult testName t stat
    whenJust _paramDataFilePrefix $ \prefix ->
        writeLatencyData prefix testName stat
#ifdef WITH_CHART
    whenJust _paramChartFilePrefix $ \prefix ->
        writeChart prefix testName stat
#endif
  where
    dyCfg = dyConfiguration
        { DY.ddbcRegion = _paramRegion
        }
    managerSettings = HTTP.defaultManagerSettings
        { HTTP.managerConnCount = _paramThreadCount + 5
        , HTTP.managerResponseTimeout = Just (1000 * 1000000) -- 1 second
        , HTTP.managerWrapIOException = id
#if MIN_VERSION_http_client(0,3,7)
        , HTTP.managerIdleConnectionCount = 512 -- this is the default
#endif
        }

-- | Use one 'Manager' per thread.
--
runTest
    :: (Transaction r x, ServiceConfiguration r ~ DY.DdbConfiguration)
    => T.Text -- ^ test name
    -> TestParams -- ^ test parameters
    -> (Int -> [r]) -- ^ requests per thread
    -> IO ()
runTest testName TestParams{..} mkRequests = do
    T.putStrLn $ "Start test \"" <> testName <> "\""
    cfg <- baseConfiguration
    (t, stats) <- time $ mapConcurrently
        (\r -> HTTP.withManager managerSettings $ \m -> runThread cfg dyCfg m r)
        (map mkRequests [0.. _paramThreadCount - 1])

    -- report results
    let stat = mconcat stats
    printResult testName t stat
    whenJust _paramDataFilePrefix $ \prefix ->
        writeLatencyData prefix testName stat
#ifdef WITH_CHART
    whenJust _paramChartFilePrefix $ \prefix ->
        writeChart prefix testName stat
#endif
  where
    dyCfg = dyConfiguration
        { DY.ddbcRegion = _paramRegion
        }
    managerSettings = HTTP.defaultManagerSettings
        { HTTP.managerConnCount = 1
        , HTTP.managerResponseTimeout = Just (1000 * 1000000) -- 1 second
        , HTTP.managerWrapIOException = id
#if MIN_VERSION_http_client(0,3,7)
        , HTTP.managerIdleConnectionCount = 1
#endif
        }

-- -------------------------------------------------------------------------- --
-- Test Vectors

putItems
    :: T.Text -- ^ table name
    -> Int -- ^ number of items per thread
    -> Int -- ^ thread Id
    -> [DY.PutItem]
putItems tableName itemsPerThread threadId = map (DY.putItem tableName) $
    testItems (sshow threadId) itemsPerThread

getItems0
    :: T.Text -- ^ table name
    -> Int -- ^ number of items per thread
    -> Int -- ^ thread Id
    -> [DY.GetItem]
getItems0 tableName itemsPerThread _ = replicate itemsPerThread $
    DY.getItem tableName $ DY.hk "Id" (DY.DString "0-0")

getItems1
    :: T.Text -- ^ table name
    -> Int -- ^ number of items per thread
    -> Int -- ^ thread Id
    -> [DY.GetItem]
getItems1 tableName itemsPerThread threadId = replicate itemsPerThread $
    DY.getItem tableName $ DY.hk "Id" (DY.DString $ sshow threadId <> "-0")

getItems2
    :: T.Text -- ^ table name
    -> Int -- ^ number of items per thread
    -> Int -- ^ thread Id
    -> [DY.GetItem]
getItems2 tableName itemsPerThread threadId = map (DY.getItem tableName) $
    testQueries (sshow threadId) itemsPerThread

-- -------------------------------------------------------------------------- --
-- Parameters

data TestParams = TestParams
    { _paramThreadCount :: !Int
    , _paramRequestCount :: !Int
    , _paramReadCapacity :: !Int
    , _paramWriteCapacity :: !Int
    , _paramTableName :: !T.Text
    , _paramKeepTable :: !Bool
    , _paramDataFilePrefix :: !(Maybe String)
#ifdef WITH_CHART
    , _paramChartFilePrefix :: !(Maybe String)
#endif
    , _paramRegion :: !DY.Region
    }
    deriving (Show, Read, Eq, Typeable)

defaultTestParams :: TestParams
defaultTestParams = TestParams
    { _paramThreadCount = 1
    , _paramRequestCount = 100
    , _paramReadCapacity = 5
    , _paramWriteCapacity = 5
    , _paramTableName = "__DYNAMODB_PERFORMANCE_TEST__"
    , _paramKeepTable = False
    , _paramDataFilePrefix = Nothing
#ifdef WITH_CHART
    , _paramChartFilePrefix = Nothing
#endif
    , _paramRegion = DY.ddbUsWest2
    }

$(makeLenses ''TestParams)

instance ToJSON TestParams where
    toJSON TestParams{..} = object
        [ "ThreadsCount" .= _paramThreadCount
        , "RequestCount" .= _paramRequestCount
        , "ReadCapacity" .= _paramReadCapacity
        , "WriteCapacity" .= _paramWriteCapacity
        , "TableName" .= _paramTableName
        , "KeepTable" .= _paramKeepTable
        , "DataFilePrefix" .= _paramDataFilePrefix
#ifdef WITH_CHART
        , "ChartFilePrefix" .= _paramChartFilePrefix
#endif
        , "Region" .= B8.unpack (DY.rName _paramRegion)
        ]

instance FromJSON (TestParams -> TestParams) where
    parseJSON = withObject "TestParams" $ \o -> id
        <$< paramThreadCount ..: "ThreadCount" % o
        <*< paramRequestCount ..: "RequestCount" % o
        <*< paramReadCapacity ..: "ReadCapacity" % o
        <*< paramWriteCapacity ..: "WriteCapacity" % o
        <*< paramTableName ..: "TableName" % o
        <*< paramKeepTable ..: "KeepTable" % o
        <*< paramDataFilePrefix ..: "DataFilePrefix" % o
#ifdef WITH_CHART
        <*< paramChartFilePrefix ..: "ChartFilePrefix" % o
#endif
        <*< setProperty paramRegion "Region" parseRegion o
      where
        parseRegion = withText "Region" $ either fail return . readRegion

pTestParams :: MParser TestParams
pTestParams = id
    <$< paramThreadCount .:: option auto
        % long "thread-count"
        <> metavar "INT"
        <> help "number of request threads"
    <*< paramRequestCount .:: option auto
        % long "request-count"
        <> metavar "INT"
        <> help "number of requests PER THREAD"
    <*< paramReadCapacity .:: option auto
        % long "read-capacity"
        <> metavar "INT"
        <> help "minimum provisioned read capacity for the test table"
    <*< paramWriteCapacity .:: option auto
        % long "write-capacity"
        <> metavar "INT"
        <> help "minimum provisioned write capacity for the test table"
    <*< paramTableName . from T.packed .:: strOption
        % long "table-name"
        <> metavar "STRING"
        <> help "name oftabel that is used for the tests. If the table does not exit it is created"
    <*< paramKeepTable .:: switch
        % long "keep-table"
        <> help "don't delete table of the test. This is always true for pre-existing tables."
    <*< paramDataFilePrefix .:: fmap Just % strOption
        % long "data-file-prefix"
        <> metavar "STRING"
        <> help "if present raw latency data is written to files with this prefix."
#ifdef WITH_CHART
    <*< paramChartFilePrefix .:: fmap Just % strOption
        % long "chart-file-prefix"
        <> metavar "STRING"
        <> help "if present latency density chargts are written to files with this prefix."
#endif
    <*< paramRegion .:: option (eitherReader (readRegion . T.pack))
        % long "region"
        <> metavar "REGION-STRING"
        <> help "the AWS region that is used for the test Dynamo database"

-- -------------------------------------------------------------------------- --
-- Main

mainInfo :: ProgramInfo TestParams
mainInfo = programInfo "Dynamo Performace Test" pTestParams defaultTestParams
    & piHelpHeader .~ Just % L.intercalate "\n"
        [ "In order to use the application you must put your AWS API credentials for"
        , "your AWS account in the file '~/.aws-keys' as described in the"
        , "Documentation of the aws package (https://github.com/aristidb/aws#example-usage)."
        ]
    & piHelpFooter .~ Just % L.intercalate "\n"
        [ "IMPORTANT NOTE:"
        , ""
        , "By using the dynamo-performace application from this package with your AWS API"
        , "credentials costs will incure to your AWS account. Depending on the provisioned"
        , "test table read and write throughput these costs can be in the order of several"
        , "dollars per hour."
        , ""
        , "Also be aware that there is an option to keep the table after the tests are finished"
        , "(for example for usage with successive test runs). If you use that option you have to"
        , "make sure that you delete the table yourself when you don't need it any more."
        ]

main :: IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \params@TestParams{..} -> do

    -- Check if table exists
    tabDesc <- (Just <$> simpleDy (DY.DescribeTable _paramTableName)) `catch` \(e :: DY.DdbError) ->
        case DY.ddbErrCode e of
            DY.ResourceNotFoundException -> return Nothing
            _ -> error $ "unexpected exception when checking for existence of table: " <> show e

    -- Prepare table
    let getTable = case (tabDesc, _paramKeepTable) of

            (Nothing, False) -> withTable_ False _paramTableName _paramReadCapacity _paramWriteCapacity

            (Nothing, True) -> \f -> do
                r <- runEitherT $ do
                    retryT 3 $ tryT $ createTestTable _paramTableName _paramReadCapacity _paramWriteCapacity
                    retryT 6 $ do
                        tableDesc <- simpleDyT $ DY.DescribeTable _paramTableName
                        when (DY.rTableStatus tableDesc == "CREATING") $ left "Table not ready: status CREATING"
                        return _paramTableName
                either (error . T.unpack) f r

            (Just DY.TableDescription{..}, _) -> \f -> do

                -- Check table
                let tableReadCapacity = DY.statusReadCapacityUnits rProvisionedThroughput
                let tableWriteCapacity = DY.statusWriteCapacityUnits rProvisionedThroughput

                unless (rTableStatus == "ACTIVE") . error $ "Table not ready: status " <> T.unpack rTableStatus

                when (tableReadCapacity < _paramReadCapacity) . error $
                    "Read capacity of table " <> T.unpack _paramTableName <> " is not enough; requested "
                    <> sshow _paramReadCapacity <> " provisioned: " <> sshow tableReadCapacity

                when (tableWriteCapacity < _paramWriteCapacity) . error $
                    "Write capacity of table " <> T.unpack _paramTableName <> " is not enough; requested "
                    <> sshow _paramWriteCapacity <> " provisioned: " <> sshow tableWriteCapacity

                -- return table
                f _paramTableName

    -- Initialize table and run tests
    getTable $ \tableName -> do
        runTest "put"  params $ putItems tableName _paramRequestCount
        runTest "get0" params $ getItems0 tableName _paramRequestCount
        runTest "get1" params $ getItems1 tableName _paramRequestCount
        runTest "get2" params $ getItems2 tableName _paramRequestCount
