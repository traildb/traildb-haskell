{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE BangPatterns #-}

module Main ( main ) where

import Control.Exception
import Control.DeepSeq
import Control.Monad
import Criterion
import Criterion.Main
import qualified Data.ByteString as B
import Data.Foldable
import Data.IORef
import Data.Monoid
import Data.Serialize.Put
import System.Directory
import System.IO.Unsafe ( unsafePerformIO )
import System.Random
import System.TrailDB

-- This hack needed to clean up after we close
createdTrailDBs :: IORef [FilePath]
createdTrailDBs = unsafePerformIO $ newIORef []
{-# NOINLINE createdTrailDBs #-}

main :: IO ()
main = flip finally clearupTrailDBs $ defaultMain
  [
    benchReadEq 1
  , benchReadEq 2
  , benchReadEq 5
  , benchReadEq 20
  , benchReadEq 150
  , benchConsEq 1
  , benchConsEq 2
  , benchConsEq 5
  , benchConsEq 20
  , benchConsEq 150
  ]

clearupTrailDBs :: IO ()
clearupTrailDBs = do
  paths <- readIORef createdTrailDBs
  for_ paths $ \path -> do
    _ <- try $ removeDirectoryRecursive path :: IO (Either SomeException ())
    _ <- try $ removeFile (path <> ".tdb") :: IO (Either SomeException ())
    return ()

instance NFData TdbCons where
  rnf !_ = ()

instance NFData Tdb where
  rnf !_ = ()

instance NFData Cursor where
  rnf !_ = ()

instance NFData (IO UUID) where
  rnf !_ = ()

benchConsEq :: Int -> Benchmark
benchConsEq num_fields = env (prepareCons num_fields) $ \ ~(cons, uuidgen, _) -> bench ("addTrail" <> show num_fields) $ whnfIO $ do
  ug <- uuidgen
  addTrail cons ug 100 item
 where
  item = replicate num_fields ("hello" :: B.ByteString)

benchReadEq :: Int -> Benchmark
benchReadEq num_fields = env (prepareTrailDB num_fields) $ \ ~(tdb, cursor) -> bench ("seekTrail" <> show num_fields) $ whnfIO $ do
   idx <- randomRIO (0, 999999)
   setCursor cursor idx
   stepCursor cursor

prepareTrailDB :: Int -> IO (Tdb, Cursor)
prepareTrailDB num_fields = do
  (cons, _, name) <- prepareCons num_fields
  closeTrailDBCons cons

  tdb <- openTrailDB name
  cursor <- makeCursor tdb

  return (tdb, cursor)

prepareCons :: Int -> IO (TdbCons, IO UUID, FilePath)
prepareCons num_fields = do
  name <- replicateM 10 $ randomRIO ('a', 'z')
  cons <- newTrailDBCons name (flip fmap [0..num_fields-1] show)
  atomicModifyIORef' createdTrailDBs $ \old -> ( name:old, () )

  uuid_ref <- newIORef 0
  let new_uuid = do idx <- readIORef uuid_ref
                    modifyIORef' uuid_ref $ \old -> old+1
                    return $ runPut $ do putWord64le idx
                                         putByteString $ B.replicate 8 0x0

  replicateM_ 1000000 $ do
    uuid <- new_uuid
    addTrail cons uuid 100 item

  return (cons, new_uuid, name)
 where
  item = replicate num_fields ("blah" :: B.ByteString)

