{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE GADTs #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE RankNTypes #-}

-- |
-- Module: Aws.Test.DynamoDb.Utils
-- Copyright: Copyright © 2014 AlephCloud Systems, Inc.
-- License: MIT
-- Maintainer: Lars Kuhtz <lars@alephcloud.com>
-- Stability: experimental
--
-- Utils for testing the Haskell bindings for Amazon DynamoDb
--

module Aws.Test.DynamoDb.Utils
(
-- * Static Parameters
  testProtocol
, testRegion
, defaultTableName

-- * Static Configuration
, dyConfiguration

-- * DynamoDb Requests
, DyT
, dyT
, memDyT
, defaultDyT

-- * Test Tables
, withTable
, defaultWithTable
, withTable_
, defaultWithTable_
, createTestTable
, defaultCreateTestTable

-- * Misc Utils
, readRegion
) where

import Aws
import Aws.Core
import qualified Aws.DynamoDb as DY
import Aws.Test.Utils

import Control.Applicative
import Control.Error
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Control.Monad.Trans.Control
import Control.Monad.Trans.Resource

import qualified Data.List as L
import Data.Monoid
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T

import qualified Network.HTTP.Client as HTTP

import System.IO

-- -------------------------------------------------------------------------- --
-- Static Test parameters
--
-- TODO make these configurable

testProtocol :: Protocol
testProtocol = HTTP

testRegion :: DY.Region
testRegion = DY.ddbUsWest2

defaultTableName :: T.Text
defaultTableName = "test-table"

-- -------------------------------------------------------------------------- --
-- DynamoDb Configuration

dyConfiguration :: DY.DdbConfiguration NormalQuery
dyConfiguration = DY.DdbConfiguration
    { DY.ddbcRegion = testRegion
    , DY.ddbcProtocol = testProtocol
    , DY.ddbcPort = Nothing
    }

-- -------------------------------------------------------------------------- --
-- DynamoDb Requests

type DyT = (Transaction r a, ServiceConfiguration r ~ DY.DdbConfiguration, MonadBaseControl IO m, MonadIO m)
    => r
    -> EitherT SomeException m a

type MemDyT =
    ( AsMemoryResponse a
    , Transaction r a
    , ServiceConfiguration r ~ DY.DdbConfiguration
    , MonadBaseControl IO m, MonadIO m
    )
    => r
    -> EitherT SomeException m (MemoryResponse a)

-- dyMemT :: DyT -> DyMemT
-- dyMemT f = f >=> liftIO . runResourceT . loadToMemory

defaultDyT :: DyT
defaultDyT command = EitherT . liftIO $ do
    cfg <- baseConfiguration
    HTTP.withManager HTTP.defaultManagerSettings $ \m -> responseResult
        <$> runResourceT (aws cfg dyConfiguration m command)

-- | Run an AWS DynamoDb request with the provided resources
--
-- All exceptions are wrapped in the resulting 'EitherT'.
--
dyT
    :: Configuration
    -> DY.DdbConfiguration NormalQuery
    -> HTTP.Manager
    -> DyT
dyT cfg dyCfg manager req = EitherT . liftIO $
    responseResult <$> runResourceT (aws cfg dyCfg manager req)

memDyT
    :: DyT
    -> MemDyT
memDyT runDyT = runDyT >=> liftIO . runResourceT . loadToMemory

{-
defaultDy
    :: (AsMemoryResponse a, Transaction r a, ServiceConfiguration r ~ DY.DdbConfiguration, MonadIO m)
    => r
    -> m (MemoryResponse a)
defaultDy command = do
    c <- baseConfiguration
    HTTP.withManager $ m ->
        responseResult <$> aws c dyConfiguration command
-}

-- -------------------------------------------------------------------------- --
-- Test Tables

defaultWithTable
    :: T.Text -- ^ table Name
    -> Int -- ^ read capacity (#(non-consistent) reads * itemsize/4KB)
    -> Int -- ^ write capacity (#writes * itemsize/1KB)
    -> (T.Text -> IO a) -- ^ test action
    -> IO a
defaultWithTable = withTable defaultDyT

withTable
    :: DyT
    -> T.Text -- ^ table Name
    -> Int -- ^ read capacity (#(non-consistent) reads * itemsize/4KB)
    -> Int -- ^ write capacity (#writes * itemsize/1KB)
    -> (T.Text -> IO a) -- ^ test action
    -> IO a
withTable run = withTable_ run True

defaultWithTable_
    :: Bool -- ^ whether to prefix the table name
    -> T.Text -- ^ table Name
    -> Int -- ^ read capacity (#(non-consistent) reads * itemsize/4KB)
    -> Int -- ^ write capacity (#writes * itemsize/1KB)
    -> (T.Text -> IO a) -- ^ test action
    -> IO a
defaultWithTable_ = withTable_ defaultDyT

withTable_
    :: DyT
    -> Bool -- ^ whether to prefix the table name
    -> T.Text -- ^ table Name
    -> Int -- ^ read capacity (#(non-consistent) reads * itemsize/4KB)
    -> Int -- ^ write capacity (#writes * itemsize/1KB)
    -> (T.Text -> IO a) -- ^ test action
    -> IO a
withTable_ runDyT prefix tableName readCapacity writeCapacity f =
    bracket_ createTable deleteTable $ f tTableName
  where
    tTableName = if prefix then testData tableName else tableName
    deleteTable = do
        r <- runEitherT . retryT 6 $
            void (memDyT runDyT $ DY.DeleteTable tTableName) `catchT` \e -> do
                liftIO . T.hPutStrLn stderr $ "attempt to delete table failed: " <> sshow e
                left e
        either (error . show) (const $ return ()) r

    createTable = do
        r <- runEitherT $ do
            retryT 3 $ createTestTable runDyT tTableName readCapacity writeCapacity
            retryT 6 $ do
                tableDesc <- memDyT runDyT $ DY.DescribeTable tTableName
                when (DY.rTableStatus tableDesc == "CREATING") $ testThrowT "Table not ready: status CREATING"
        either (error . show) return r

defaultCreateTestTable
    :: T.Text -- ^ table Name
    -> Int -- ^ read capacity (#(non-consistent) reads * itemsize/4KB)
    -> Int -- ^ write capacity (#writes * itemsize/1KB)
    -> EitherT SomeException IO ()
defaultCreateTestTable = createTestTable defaultDyT

createTestTable
    :: DyT
    -> T.Text -- ^ table Name
    -> Int -- ^ read capacity (#(non-consistent) reads * itemsize/4KB)
    -> Int -- ^ write capacity (#writes * itemsize/1KB)
    -> EitherT SomeException IO ()
createTestTable runDyT tableName readCapacity writeCapacity = void . memDyT runDyT $
    DY.createTable
        tableName
        attrs
        (DY.HashOnly keyName)
        throughPut
  where
    keyName = "Id"
    keyType = DY.AttrString
    attrs = [DY.AttributeDefinition keyName keyType]
    throughPut = DY.ProvisionedThroughput
        { DY.readCapacityUnits = readCapacity
        , DY.writeCapacityUnits = writeCapacity
        }

-- -------------------------------------------------------------------------- --
-- Misc Utils

readRegion
    :: T.Text
    -> Either String DY.Region
readRegion t =
    maybe (Left $ "unknown region: " <> T.unpack t) Right $
        L.find (\(DY.Region _ n) -> T.decodeUtf8 n == t)
            [ DY.ddbLocal
            , DY.ddbUsEast1
            , DY.ddbUsWest1
            , DY.ddbUsWest2
            , DY.ddbEuWest1
            , DY.ddbApNe1
            , DY.ddbApSe1
            , DY.ddbApSe2
            , DY.ddbSaEast1
            ]
