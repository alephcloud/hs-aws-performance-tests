{-# LANGUAGE UnicodeSyntax #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE CPP #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE FlexibleContexts #-}

-- |
-- Module: WarpDynamo
-- Copyright: Copyright © 2014 AlephCloud Systems, Inc.
-- License: MIT
-- Maintainer: Lars Kuhtz <lars@alephcloud.com>
-- Stability: experimental
--
-- Test performance of Warp in conjunction with DynamoDb
--
module Main -- Aws.Test.DynamoDb.DynamoDbWarpPerformance
( main
) where

import Control.Concurrent.Async
import Control.Monad

import Data.Aeson.Types (parseEither)
import qualified Data.ByteString.Lazy.Char8 as LB8
import qualified Data.ByteString as B
import Data.Default
import Data.IORef

import Network.HTTP.Client
import Network.HTTP.Types
import qualified Network.Wai as W
import qualified Network.Wai.Handler.Warp as Warp

--

import Aws
import qualified Aws.DynamoDb as DY

import Aws.Test.Utils
import Aws.Test.DynamoDb.Utils

import Configuration.Utils

import Control.Error
import Control.Exception
import Control.Lens hiding (act, (.=))

import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lens as B
import qualified Data.CaseInsensitive as CI
import qualified Data.List as L
import qualified Data.Map as M
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.IO as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lens as T
import Data.Typeable

import qualified Network.HTTP.Client as HTTP

import PkgInfo

-- -------------------------------------------------------------------------- --
-- Common Functions

-- -------------------------------------------------------------------------- --
-- Parameters

data TestParams = TestParams
    { _paramThreadCount ∷ !Int
    , _paramRequestCount ∷ !Int
    , _paramReadCapacity ∷ !Int
    , _paramWriteCapacity ∷ !Int
    , _paramTableName ∷ !T.Text
    , _paramKeepTable ∷ !Bool
    , _paramDataFilePrefix ∷ !(Maybe String)
#ifdef WITH_CHART
    , _paramChartFilePrefix ∷ !(Maybe String)
#endif
    , _paramRegion ∷ !DY.Region
    }
    deriving (Show, Read, Eq, Typeable)

defaultTestParams ∷ TestParams
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
        [ "ThreadCount" .= _paramThreadCount
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

instance FromJSON (TestParams → TestParams) where
    parseJSON = withObject "TestParams" $ \o → id
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

pTestParams ∷ MParser TestParams
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
    <*< paramKeepTable .:: option (eitherReader boolReader)
        % long "keep-table"
        <> metavar "true|false"
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
  where
    boolReader s = case CI.mk (T.pack s) of
        "true" -> Right True
        "false" -> Right False
        _ -> Left $ "failed to read Boolean value: " <> s

-- -------------------------------------------------------------------------- --
-- Server Test Parameters

data ServerTestMode
    = ClientMode
    | ServerMode
    deriving (Show, Read, Eq, Enum, Bounded, Typeable)

instance ToJSON ServerTestMode where
    toJSON ClientMode = "client"
    toJSON ServerMode = "server"

instance FromJSON ServerTestMode where
    parseJSON = withText "ServerTestMode" $ \case
        "server" → return ServerMode
        "client" → return ClientMode
        x → fail $ "unsupported test mode" <> T.unpack x

data ServerParams = ServerParams
    { _serverTestParams ∷ !TestParams
    , _serverHost ∷ !B.ByteString
    , _serverPort ∷ !Int
    , _serverMode ∷ !ServerTestMode
    }
    deriving (Show, Read, Eq, Typeable)

$(makeLenses ''ServerParams)

defaultServerParams ∷ ServerParams
defaultServerParams = ServerParams
    { _serverTestParams = defaultTestParams
    , _serverHost = "127.0.0.1"
    , _serverPort = 8080
    , _serverMode = ClientMode
    }

instance ToJSON ServerParams where
    toJSON ServerParams{..} = object
        [ "testParams" .= _serverTestParams
        , "host" .= B8.unpack _serverHost
        , "port" .= _serverPort
        , "mode" .= _serverMode
        ]

instance FromJSON (ServerParams → ServerParams) where
    parseJSON = withObject "ServerParams" $ \o → id
        <$< serverTestParams %.: "testParams" % o
        <*< serverHost . from B.packedChars ..: "host" % o
        <*< serverPort ..: "port" % o
        <*< serverMode ..: "mode" % o

pServerParams ∷ MParser ServerParams
pServerParams = id
    <$< serverTestParams %:: pTestParams
    <*< serverHost . from B.packedChars .:: strOption
        % long "host"
        <> metavar "HOSTNAME"
        <> help "hostname of the test HTTP server"
    <*< serverPort .:: option auto
        % long "port"
        <> metavar "INT"
        <> help "port of the test HTTP server"
    <*< serverMode .:: option (eitherReader modeReader)
        % long "mode"
        <> metavar "server|client"
        <> help "whether to run as test server or test client."
  where
    modeReader "server" = Right ServerMode
    modeReader "client" = Right ClientMode
    modeReader a = Left $ "unsupported mode: " <> a

-- -------------------------------------------------------------------------- --
-- Server

server
    ∷ ServerParams
    → IO ()
server params = do

    -- ---------------------------------------- --
    -- initialize table(s)
    T.putStrLn "preparing test table"
    withTestTable (_serverTestParams params) $ \tableName → do

        -- ---------------------------------------- --
        -- initialize metrics

        -- counts number of outgoing TCP connections
        refTcpConnections ← newIORef (0 ∷ Int)
        refStat ← newIORef (mempty ∷ Stat)

        -- ---------------------------------------- --
        -- run server API
        do
            HTTP.withManager (managerSettings refTcpConnections) $ \manager → do
                cfg ← baseConfiguration
                let runDyT ∷ DyT
                    runDyT = dyT cfg dyCfg manager
                T.putStrLn $ "starting server on port " <> sshow (_serverPort params)
                Warp.run (_serverPort params) $ serverApp runDyT tableName refStat

            -- ---------------------------------------- --
            -- On Shutdown (finally)

            -- write metrics

            -- generate reports
            `finally` do
                print =<< readIORef refStat
                print =<< readIORef refTcpConnections

  where
    -- A manager that counts TCP connections
    managerSettings ref = defaultManagerSettings
        { managerRawConnection = do
            mkConn ← managerRawConnection defaultManagerSettings
            return $ \a b c → do
                atomicModifyIORef ref $ \i → (succ i, ())
                mkConn a b c
        }

    dyCfg ∷ DY.DdbConfiguration NormalQuery
    dyCfg = dyConfiguration
        { DY.ddbcRegion = (_paramRegion . _serverTestParams) params
        }

serverApp
    ∷ DyT
    → T.Text -- ^ table name
    → IORef Stat
    → W.Application
serverApp runAws tableName statRef req respond = do
    (t, result) ← time $ serverAPI runAws tableName req
    response ← case result of
        Right r → do
            logSuccess statRef t
            return $ W.responseLBS status200 [("content-type", "application/json")] $ encode r
        Left e → do
            logFailure statRef t e
            return $ W.responseLBS status500 [("content-type", "text/plain")] . LB8.fromStrict $ T.encodeUtf8 e
    respond response

serverAPI
    ∷ DyT
    → T.Text -- ^ table name
    → W.Request
    → IO (Either T.Text Value)

serverAPI runDyT tableName req

    -- GET
    | W.requestMethod req == "GET", [keyId,keyValue] ← W.pathInfo req = runEitherT $ do
        r ← fmapLT sshow $ memDyT runDyT . DY.getItem tableName $ DY.hk keyId (DY.DString keyValue)
        case DY.girItem r of
            Nothing → left "not found"
            Just item → right $ toJSON item

    | W.requestMethod req == "GET" = return . Left $
        "unsupported query: " <> sshow (W.rawPathInfo req)

    -- PUT (item is in the body as JSON value)
    | W.requestMethod req == "PUT" = runEitherT $ fmapLT T.pack $ do
        bodyValue ← EitherT $ eitherDecode <$> W.strictRequestBody req
        bodyAttr ← hoistEither $ parseEither DY.parseAttributeJson bodyValue
        fmap (const $ object []) . fmapLT sshow $
            memDyT runDyT . DY.putItem tableName $ DY.item bodyAttr

    | otherwise = return . Left $
        "unsupported request method " <> sshow (W.requestMethod req)

-- -------------------------------------------------------------------------- --
-- Test Table

withTestTable
    ∷ TestParams
    → (T.Text → IO a)
    → IO a
withTestTable TestParams{..} f = HTTP.withManager managerSettings $ \manager → do

    cfg ← baseConfiguration
    let runDyT ∷ DyT
        runDyT = dyT cfg dyCfg manager

    -- Check if table exists
    tabDesc ← (Just <$> memDyT runDyT (DY.DescribeTable _paramTableName))
        `fromEitherET_` \x → case fmap DY.ddbErrCode x of
            Right DY.ResourceNotFoundException → return Nothing
            Right e → error $ "unexpected exception when checking for existence of table: " <> show e
            Left e → error $ "unexpected exception when checking for existence of table: " <> show e
        -- \(e ∷ Maybe DY.DdbError) ->

    -- Prepare table
    case (tabDesc, _paramKeepTable) of

        (Nothing, False) → withTable_ runDyT False _paramTableName _paramReadCapacity _paramWriteCapacity f

        (Nothing, True) → do
            r ← runEitherT $ do
                retryT 3 $ createTestTable runDyT _paramTableName _paramReadCapacity _paramWriteCapacity
                retryT 6 $ do
                    tableDesc ← memDyT runDyT $ DY.DescribeTable _paramTableName
                    when (DY.rTableStatus tableDesc == "CREATING") $ testThrowT "Table not ready: status CREATING"
                    return _paramTableName
            either (error . show) f r

        (Just DY.TableDescription{..}, _) → do

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
  where
    dyCfg ∷ DY.DdbConfiguration NormalQuery
    dyCfg = dyConfiguration
        { DY.ddbcRegion = _paramRegion
        }
    managerSettings = HTTP.defaultManagerSettings

-- -------------------------------------------------------------------------- --
-- Client

runThread
    ∷ HTTP.Manager
    → [HTTP.Request]
    → IO Stat
runThread manager = flip foldM mempty $ \stat req → do
    (t,response) ← time . try $ httpLbs req manager
    case response of
        Right _ → return $ stat <> successStat (realToFrac t * 1000)
        -- FIXME we should also catch IO exceptions, I guess.
        Left (e ∷ HTTP.HttpException) → return $ stat <> failStat (realToFrac t * 1000) (sshow e)

-- | Use one 'Manager' per thread.
--
runTest
    ∷ T.Text -- ^ test name
    → ServerParams -- ^ test parameters
    → (HTTP.Request → Int → [HTTP.Request]) -- ^ requests per thread
    → IO ()
runTest testName ServerParams{..} mkRequests = do
    T.putStrLn $ "Start test \"" <> testName <> "\""
    (t, stats) ← time $ mapConcurrently
        (\r → HTTP.withManager managerSettings $ \m → runThread m r)
        (map (mkRequests req) [0.. (_paramThreadCount _serverTestParams) - 1])

    -- report results
    let stat = mconcat stats
    printStat testName t stat
    whenJust (_paramDataFilePrefix _serverTestParams) $ \prefix ->
        writeStatFiles prefix testName stat
#ifdef WITH_CHART
    whenJust (_paramChartFilePrefix _serverTestParams) $ \prefix ->
        writeStatChart prefix testName stat
#endif
  where
    req = def
        { HTTP.method = "GET"
        , HTTP.secure = False
        , HTTP.host = _serverHost
        , HTTP.port = _serverPort
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

testItems ∷ T.Text → Int → [DY.Item]
testItems prefix n = map (\i → M.singleton "Id" (DY.DString $ prefix <> "-" <> sshow i)) [0..n-1]

testQueries ∷ T.Text → Int → [(B.ByteString, B.ByteString)]
testQueries prefix n = map (\i → ("Id", T.encodeUtf8 prefix <> "-" <> sshow i)) [0..n-1]

putItems
    ∷ Int -- ^ number of items per thread
    → HTTP.Request -- ^ request template
    → Int -- ^ thread Id
    → [Request]
putItems itemsPerThread req threadId =
    map newReq $ testItems (sshow threadId) itemsPerThread
  where
    newReq i = req
        { HTTP.requestBody = RequestBodyLBS . encode . DY.attributesJson . DY.attributes $ i
        , HTTP.method = "PUT"
        }

getItems0
    ∷ Int -- ^ number of items per thread
    → HTTP.Request -- ^ request template
    → Int -- ^ thread Id
    → [Request]
getItems0 itemsPerThread req _ = replicate itemsPerThread $ req
    { HTTP.method = "GET"
    , HTTP.path = "Id/0-0"
    }
    -- DY.getItem tableName $ DY.hk "Id" (DY.DString "0-0")

getItems1
    ∷ Int -- ^ number of items per thread
    → HTTP.Request -- ^ request template
    → Int -- ^ thread Id
    → [Request]
getItems1 itemsPerThread req threadId = replicate itemsPerThread $ req
    { HTTP.method = "GET"
    , HTTP.path = "Id/" <> sshow threadId <> "-0"
    }

getItems2
    ∷ Int -- ^ number of items per thread
    → HTTP.Request -- ^ request template
    → Int -- ^ thread Id
    → [Request]
getItems2 itemsPerThread req threadId = map newReq $
    testQueries (sshow threadId) itemsPerThread
  where
    newReq (key,val) = req
        { HTTP.method = "GET"
        , HTTP.path = key <> "/" <> val
        }

-- -------------------------------------------------------------------------- --
-- Main

mainInfo ∷ ProgramInfo ServerParams
mainInfo = programInfo "Dynamo Performace Test" pServerParams defaultServerParams
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

main ∷ IO ()
main = runWithPkgInfoConfiguration mainInfo pkgInfo $ \params → do
    case _serverMode params of
        ServerMode → server params
        ClientMode → client params

client ∷ ServerParams → IO ()
client params = do
    runTest "put" params $ putItems c
    runTest "get0" params $ getItems0 c
    runTest "get1" params $ getItems1 c
    runTest "get2" params $ getItems2 c
  where
    c = _paramRequestCount $ _serverTestParams params
