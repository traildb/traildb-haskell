{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveTraversable #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE DeriveFoldable #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DeriveFunctor #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE ViewPatterns #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE RankNTypes #-}

-- | Haskell bindings to trailDB library.
--
-- Minimal program that lists a TrailDB:
--
-- @
--   import qualified Data.ByteString as B
--   import System.TrailDB
--
--   main :: IO ()
--   main = do
--     tdb <- openTrailDB "wikipedia-history-small.tdb"
--     forEachTrailID tdb $ \tid -> print =<< getTrailBytestring tdb tid
-- @
--
-- Example program that reads a TrailDB using low-level (faster) cursor API:
--
-- @
--   import qualified Data.ByteString as B
--   import qualified Data.Vector.Unboxed as V
--   import System.TrailDB
--
--   main :: IO ()
--   main = do
--     tdb <- openTrailDB "some-trail-db"
--     number_of_trails <- getNumTrails tdb
--
--     let arbitrarily_chosen_trail_id = 12345 \`mod\` number_of_trails
--
--     cursor <- makeCursor tdb
--     setCursor cursor arbitrarily_chosen_trail_id
--
--     -- Read the first event in the arbitrary chosen trail
--     crumb <- stepCursor cursor
--     case crumb of
--       Nothing -> putStrLn "Cannot find this particular trail."
--       Just (timestamp, features) ->
--         V.forM_ features $ \feature -> do
--           field_name <- getFieldName tdb (feature^.field)
--           putStr "Field: "
--           B.putStr field_name
--           putStr " contains value "
--           value <- getValue tdb feature
--           B.putStrLn value
--
-- @
--
-- Another example program that writes a TrailDB:
--
-- @
--   {-\# LANGUAGE OverloadedStrings \#-}
--
--   import System.TrailDB
--
--   main :: IO ()
--   main = do
--     cons <- newTrailDBCons "some-trail-db" (["currency", "order_amount", "item"] :: [String])
--     addTrail cons ("aaaaaaaaaaaaaa00")   -- UUIDs are 16 bytes in length
--                   1457049455             -- This is timestamp
--                   [\"USD\", "10.14", "Bacon & Cheese" :: String]
--     addTrail cons ("aaaaaaaaaaaaaa00")   -- Same UUID as above, same customer ordered more
--                   1457051221
--                   [\"USD\", "8.90", "Avocado Sandwich" :: String]
--     addTrail cons ("aaaaaaaaaaaaaa02")
--                   1457031239
--                   [\"JPY\", "2900", "Sun Lotion" :: String]
--     closeTrailDBCons cons
--
--     -- TrailDB has been written to 'some-trail-db'
-- @
--

module System.TrailDB
  ( 
  -- * Constructing new TrailDBs
    newTrailDBCons
  , closeTrailDBCons
  , withTrailDBCons
  , addTrail
  , appendTdbToTdbCons
  , finalizeTrailDBCons
  -- ** Supplying values to construction
  , ToTdbRow(..)
  , ToTdbRowField(..)
  , TdbConsRow(..)
  , TdbShowable(..)
  , pattern TShow
  -- * Opening existing TrailDBs
  , openTrailDB
  , closeTrailDB
  , getTdbVersion
  , dontneedTrailDB
  , willneedTrailDB
  , withTrailDB
  -- * Accessing TrailDBs
  -- ** High-level, slow, access
  , FromTrail(..)
  , getTrail
  , getTrailBytestring
  -- ** Lowerish-level, fast, access
  , makeCursor
  , stepCursor
  , stepCursorList
  , setCursor
  -- ** Iterating over TrailDB
  , forEachTrailID
  , forEachTrailIDUUID
  , traverseEachTrailID
  , traverseEachTrailIDUUID
  , foldTrailDB
  , foldTrailDBUUID
  -- ** Basic querying
  , getNumTrails
  , getNumEvents
  , getNumFields
  , getMinTimestamp
  , getMaxTimestamp
  -- ** UUID handling
  , getUUID
  , getTrailID
  -- ** Fields
  , getFieldName
  , getFieldID
  , getItemByField
  , getValue
  , getItem
  -- * Time handling
  , utcTimeToUnixTime
  , posixSecondsToUnixTime
  , dayToUnixTime
  -- * C interop
  , withRawTdb
  , getRawTdb
  , touchTdb
  , withRawTdbCons
  , getRawTdbCons
  , touchTdbCons
  , TdbRaw
  , TdbConsRaw
  -- ** Taking apart `Feature`
  , field
  , value
  , (^.)
  -- * Data types
  , UUID
  , TrailID
  , FieldID
  , Crumb
  , Feature()
  , TdbField
  , TdbVal
  , TdbVersion
  , FieldName
  , FieldNameLike(..)
  , featureWord
  , featureTdbVal
  , Cursor()
  , TdbCons()
  , Tdb()
  -- ** Time
  , UnixTime
  , getUnixTime
  -- * Exceptions
  , TrailDBException(..)
  -- * Multiple TrailDBs
  --
  -- | Operations in this section are conveniences that may be useful if you
  --   have many TrailDB directories you want to query.
  , findTrailDBs
  , filterTrailDBDirectories )
  where

import Control.Applicative
import Control.Concurrent
import Control.Monad
import Control.Monad.Catch
import Control.Monad.IO.Class
import Control.Monad.Primitive
import Control.Monad.Trans.State.Strict
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Lazy as BL
import Data.Coerce
import Data.Foldable ( for_, foldlM, Foldable )
import Data.Data
import Data.IORef
import qualified Data.Map.Strict as M
import Data.Monoid
import Data.Profunctor
import qualified Data.Set as S
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Lazy as TL
import Data.Time
import Data.Time.Clock.POSIX
import qualified Data.Vector as VS
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed as V
import qualified Data.Vector.Unboxed.Mutable as VM
import Data.Traversable ( for )
import Data.Word
import Foreign.C.String
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.Storable
import GHC.Generics
import System.Directory hiding ( isSymbolicLink )
import System.IO.Error
import System.Posix.Files.ByteString

import System.TrailDB.Error
import System.TrailDB.Internal

-- Raw types (tdb_types.h)
type TdbField = Word32
-- | `FieldID` is indexes a field number.
type FieldID = TdbField
type TdbVal = Word64
type TdbItem = Word64

foreign import ccall unsafe tdb_error_str
  :: CInt -> IO (Ptr CChar)
foreign import ccall unsafe tdb_cons_init
  :: IO (Ptr TdbConsRaw)
foreign import ccall unsafe tdb_cons_open
  :: Ptr TdbConsRaw
  -> Ptr CChar
  -> Ptr (Ptr CChar)
  -> Word64
  -> IO CInt
foreign import ccall unsafe tdb_cons_close
  :: Ptr TdbConsRaw -> IO ()
foreign import ccall unsafe tdb_cons_add
  :: Ptr TdbConsRaw
  -> Ptr Word8
  -> Word64
  -> Ptr (Ptr CChar)
  -> Ptr Word64
  -> IO CInt
foreign import ccall safe tdb_cons_finalize
  :: Ptr TdbConsRaw
  -> Word64
  -> IO CInt
foreign import ccall safe tdb_cons_append
  :: Ptr TdbConsRaw
  -> Ptr TdbRaw
  -> IO CInt
foreign import ccall unsafe tdb_dontneed
  :: Ptr TdbRaw
  -> IO ()
foreign import ccall unsafe tdb_willneed
  :: Ptr TdbRaw
  -> IO ()
foreign import ccall unsafe tdb_init
  :: IO (Ptr TdbRaw)
foreign import ccall safe tdb_open
  :: Ptr TdbRaw
  -> Ptr CChar
  -> IO CInt
foreign import ccall safe tdb_close
  :: Ptr TdbRaw
  -> IO ()
foreign import ccall unsafe tdb_get_trail_id
  :: Ptr TdbRaw
  -> Ptr Word8
  -> Ptr Word64
  -> IO CInt
foreign import ccall unsafe tdb_get_uuid
  :: Ptr TdbRaw
  -> Word64
  -> IO (Ptr Word8)
foreign import ccall unsafe tdb_num_trails
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_num_events
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_num_fields
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_min_timestamp
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_max_timestamp
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_version
  :: Ptr TdbRaw -> IO Word64
foreign import ccall unsafe tdb_get_field
  :: Ptr TdbRaw
  -> Ptr CChar
  -> Ptr TdbField
  -> IO CInt
foreign import ccall unsafe tdb_get_field_name
  :: Ptr TdbRaw
  -> TdbField
  -> IO (Ptr CChar)
foreign import ccall unsafe tdb_get_item_value
  :: Ptr TdbRaw
  -> TdbItem
  -> Ptr Word64
  -> IO (Ptr CChar)
foreign import ccall unsafe tdb_get_item
  :: Ptr TdbRaw
  -> TdbField
  -> Ptr CChar
  -> Word64
  -> IO TdbItem
foreign import ccall unsafe tdb_get_trail
  :: Ptr TdbCursorRaw
  -> Word64
  -> IO CInt
foreign import ccall unsafe tdb_cursor_new
  :: Ptr TdbRaw
  -> IO (Ptr TdbCursorRaw)
foreign import ccall unsafe tdb_cursor_free
  :: Ptr TdbCursorRaw
  -> IO ()
foreign import ccall unsafe shim_tdb_cursor_next
  :: Ptr TdbCursorRaw
  -> IO (Ptr TdbEventRaw)
foreign import ccall unsafe shim_tdb_item_to_field
  :: Word64
  -> Word32
foreign import ccall unsafe shim_tdb_item_to_val
  :: Word64
  -> Word64
foreign import ccall unsafe shim_tdb_field_val_to_item
  :: Word32
  -> Word64
  -> Word64

data TdbCursorRaw
data TdbEventRaw

-- | UUIDs should be 16 bytes in size. It can be converted to `TrailID` within a traildb.
type UUID = B.ByteString
-- | Fields names are bytestring and can contain nulls.
type FieldName = B.ByteString
-- | The type of time used in traildbs.
type UnixTime = Word64
-- | `TrailID` indexes a trail in a traildb. It can be converted to and back to
-- `UUID` within a traildb.
type TrailID = Word64
-- | TrailDB version
type TdbVersion = Word64

-- | A single crumb is some event at certain time.
--
-- The vector always has length as told by `getNumFields`.
type Crumb = (UnixTime, V.Vector Feature)

-- | `Feature` is a value in traildb. `getValue` can turn it into a
-- human-readable value within a traildb.
newtype Feature = Feature TdbItem
  deriving ( Eq, Ord, Show, Read, Typeable, Data, Generic, Storable )

-- | Type synonym from lens package. Defined here so we can avoid lens dependency.
type Iso s t a b = forall p f. (Profunctor p, Functor f) => p a (f b) -> p s (f t) 
type Iso' s a = Iso s s a a

iso :: (s -> a) -> (b -> t) -> Iso s t a b
iso sa bt = dimap sa (fmap bt)
{-# INLINE iso #-}

-- | Type synonym from lens package.
type Lens s t a b = forall f. Functor f => (a -> f b) -> s -> f t 
type Lens' s a = Lens s s a a

lens :: (s -> a) -> (s -> b -> t) -> Lens s t a b
lens sa sbt afb s = sbt s <$> afb (sa s)
{-# INLINE lens #-}



(^.) :: s -> ((a -> Const a a) -> s -> Const a s) -> a
s ^. l = getConst (l Const s)
{-# INLINE (^.) #-}

featureWord :: Iso' Feature Word64
featureWord = iso (\(Feature w) -> w) Feature
{-# INLINE featureWord #-}

featureTdbVal :: Iso' Feature TdbVal
featureTdbVal = featureWord
{-# INLINE featureTdbVal #-}

newtype instance V.Vector Feature = V_Feature (V.Vector Word64)
newtype instance VM.MVector s Feature = VM_Feature (VM.MVector s Word64)

instance VGM.MVector VM.MVector Feature where
  {-# INLINE basicLength #-}
  basicLength (VM_Feature w64) = VGM.basicLength w64
  {-# INLINE basicUnsafeSlice #-}
  basicUnsafeSlice a b (VM_Feature w64) = coerce $
    VGM.basicUnsafeSlice a b w64
  {-# INLINE basicOverlaps #-}
  basicOverlaps (VM_Feature w1) (VM_Feature w2) = VGM.basicOverlaps w1 w2
  {-# INLINE basicUnsafeNew #-}
  basicUnsafeNew sz = do
    result <- VGM.basicUnsafeNew sz
    return $ VM_Feature result
  {-# INLINE basicUnsafeRead #-}
  basicUnsafeRead (VM_Feature w64) i = do
    result <- VGM.basicUnsafeRead w64 i
    return $ coerce result
  {-# INLINE basicUnsafeWrite #-}
  basicUnsafeWrite (VM_Feature w64) i v =
    VGM.basicUnsafeWrite w64 i (coerce v)

  basicInitialize (VM_Feature w64) =
    VGM.basicInitialize w64
  {-# INLINE basicInitialize #-}

instance VG.Vector V.Vector Feature where
  {-# INLINE basicLength #-}
  basicLength (V_Feature w64) = VG.basicLength w64
  {-# INLINE basicUnsafeFreeze #-}
  basicUnsafeFreeze (VM_Feature w64) = do
    result <- VG.basicUnsafeFreeze w64
    return $ coerce (result :: V.Vector Word64)
  {-# INLINE basicUnsafeThaw #-}
  basicUnsafeThaw (V_Feature w64) = do
    result <- VG.basicUnsafeThaw w64
    return $ coerce result
  {-# INLINE basicUnsafeIndexM #-}
  basicUnsafeIndexM (V_Feature w64) idx = do
    result <- VG.basicUnsafeIndexM w64 idx
    return $ coerce result
  {-# INLINE basicUnsafeSlice #-}
  basicUnsafeSlice i1 i2 (V_Feature w64) =
    coerce $ VG.basicUnsafeSlice i1 i2 w64

-- | `Feature` is isomorphic to `Word64` so it's safe to coerce between them. (see `featureWord`).
instance V.Unbox Feature

-- | A helper function to get the current unix time.
--
-- May be useful when building TrailDBs if you don't have timestamps already.
getUnixTime :: MonadIO m => m UnixTime
getUnixTime = liftIO $ do
  now <- getPOSIXTime
  let t = floor now
  return t
{-# INLINE getUnixTime #-}

-- | Converts `UTCTime` to `UnixTime`
utcTimeToUnixTime :: UTCTime -> UnixTime
utcTimeToUnixTime utc = floor $ utcTimeToPOSIXSeconds utc

-- | Converts `POSIXTime` to `UnixTime`
posixSecondsToUnixTime :: POSIXTime -> UnixTime
posixSecondsToUnixTime = floor

-- | Converts `Day` to `UnixTime`.
--
-- The time will be the first second of the `Day`.
dayToUnixTime :: Day -> UnixTime
dayToUnixTime day = utcTimeToUnixTime (UTCTime day 0)

field :: Lens' Feature FieldID
field = lens get_it set_it
 where
  get_it (Feature f) = shim_tdb_item_to_field f
  set_it original new = Feature $ shim_tdb_field_val_to_item new (original^.value)
{-# INLINE field #-}

value :: Lens' Feature TdbVal
value = lens get_it set_it
 where
  get_it (Feature f) = shim_tdb_item_to_val f
  set_it original new = Feature $ shim_tdb_field_val_to_item (original^.field) new
{-# INLINE value #-}

-- | Class of things that can be used as a field name.
--
-- The strict bytestring is the native type. Other types are converted,
-- encoding with UTF-8.
class FieldNameLike a where
  encodeToFieldName :: a -> B.ByteString
 
instance FieldNameLike String where
  encodeToFieldName = T.encodeUtf8 . T.pack

instance FieldNameLike B.ByteString where
  encodeToFieldName = id
  {-# INLINE encodeToFieldName #-}
 
instance FieldNameLike BL.ByteString where
  encodeToFieldName = BL.toStrict

instance FieldNameLike T.Text where
  encodeToFieldName = T.encodeUtf8

instance FieldNameLike TL.Text where
  encodeToFieldName = T.encodeUtf8 . TL.toStrict

newtype TdbCons = TdbCons (MVar (Maybe (Ptr TdbConsRaw)))
  deriving ( Typeable, Generic )

tdbThrowIfError :: MonadIO m => m CInt -> m ()
tdbThrowIfError action = do
  result <- action
  unless (result == 0) $ liftIO $ do
    err_string <- peekCString =<< tdb_error_str result
    throwM $ TrailDBError result err_string

-- | Create a new TrailDB and return TrailDB construction handle.
--
-- Close it with `closeTrailDBCons`. Garbage collector will close it eventually
-- if you didn't do it yourself. You won't be receiving `FinalizationFailure`
-- exception though if that fails when using the garbage collector.
newTrailDBCons :: (FieldNameLike a, MonadIO m)
               => FilePath
               -> [a]
               -> m TdbCons
newTrailDBCons filepath fields' = liftIO $ mask_ $
  withCString filepath $ \root ->
    withBytestrings fields $ \fields_ptr -> do
      tdb_cons <- tdb_cons_init
      when (tdb_cons == nullPtr) $
        throwM CannotAllocateTrailDBCons

      flip onException (tdb_cons_close tdb_cons) $ do
        tdbThrowIfError $ tdb_cons_open
            tdb_cons
            root
            fields_ptr
            (fromIntegral $ length fields)

        -- MVar will protect the handle from being used in multiple threads
        -- simultaneously.
        mvar <- newMVar (Just tdb_cons)

        -- Make garbage collector close it if it wasn't already.
        void $ mkWeakMVar mvar $ modifyMVar_ mvar $ \case
          Nothing -> return Nothing
          Just ptr -> do
            void $ tdb_cons_finalize ptr 0
            tdb_cons_close ptr
            return Nothing

        return $ TdbCons mvar
 where
  fields = fmap encodeToFieldName fields'

-- | Runs an `IO` action with an opened `TdbCons`. The `TdbCons` is closed
-- after the action has been executed.
withTrailDBCons :: (FieldNameLike a, MonadIO m, MonadMask m)
                => FilePath
                -> [a]
                -> (TdbCons -> m b)
                -> m b
withTrailDBCons filepath fields action = mask $ \restore -> do
  cons <- newTrailDBCons filepath fields
  finally (restore $ action cons) (closeTrailDBCons cons)
  
withBytestrings :: forall a. [B.ByteString] -> (Ptr (Ptr CChar) -> IO a) -> IO a
withBytestrings [] action = action nullPtr
withBytestrings listing action =
  allocaArray (length listing) $ \bs_ptr -> loop_it bs_ptr listing 0
 where
  loop_it :: Ptr (Ptr CChar) -> [B.ByteString] -> Int -> IO a
  loop_it bs_ptr (bs:rest) idx =
    B.useAsCString bs $ \string_ptr -> do
      pokeElemOff bs_ptr idx string_ptr
      loop_it bs_ptr rest (idx+1)
  loop_it bs_ptr [] _ = action bs_ptr

-- | Class of things that can be turned into rows and added with `addTrail`.
--
-- The native type inside traildb is the `ByteString`. The use of this
-- typeclass can eliminate some noise when converting values to `ByteString`.
class ToTdbRow r where
  toTdbRow :: r -> [B.ByteString]

-- | Class of things that can be turned into a field in a TrailDB.
class ToTdbRowField f where
  toTdbField :: f -> B.ByteString

instance (ToTdbRowField f1, ToTdbRowField f2) => ToTdbRow (f1, f2) where
  toTdbRow (f1, f2) = toTdbRow [toTdbField f1, toTdbField f2]

instance (ToTdbRowField f1, ToTdbRowField f2, ToTdbRowField f3) => ToTdbRow (f1, f2, f3) where
  toTdbRow (f1, f2, f3) = toTdbRow [toTdbField f1, toTdbField f2, toTdbField f3]

instance (ToTdbRowField f1, ToTdbRowField f2, ToTdbRowField f3, ToTdbRowField f4) => ToTdbRow (f1, f2, f3, f4) where
  toTdbRow (f1, f2, f3, f4) = toTdbRow [toTdbField f1, toTdbField f2, toTdbField f3, toTdbField f4]

instance (ToTdbRowField f1, ToTdbRowField f2, ToTdbRowField f3, ToTdbRowField f4, ToTdbRowField f5) => ToTdbRow (f1, f2, f3, f4, f5) where
  toTdbRow (f1, f2, f3, f4, f5) = toTdbRow [toTdbField f1, toTdbField f2, toTdbField f3, toTdbField f4, toTdbField f5]

instance ToTdbRowField B.ByteString where
  toTdbField = id
  {-# INLINE toTdbField #-}

instance ToTdbRowField BL.ByteString where
  toTdbField = BL.toStrict
  {-# INLINE toTdbField #-}

instance ToTdbRowField String where
  toTdbField = T.encodeUtf8 . T.pack
  {-# INLINE toTdbField #-}

instance ToTdbRowField T.Text where
  toTdbField = T.encodeUtf8
  {-# INLINE toTdbField #-}

instance ToTdbRowField TL.Text where
  toTdbField = T.encodeUtf8 . TL.toStrict
  {-# INLINE toTdbField #-}

instance ToTdbRowField f => ToTdbRow [f] where
  toTdbRow = fmap toTdbField
  {-# INLINE toTdbRow #-}

-- | Convenience type that lets you arbitrarily make heterogenous list of
-- things that implement `ToTdbRowField` and subsequently `ToTdbRow`. Use this
-- if plain lists are not suitable (because they are monotyped) or your tuples
-- are too long to implement `ToTdbRow`.
data TdbConsRow a b = (:.) a b

infixr 7 :.

instance (ToTdbRowField a, ToTdbRow b)
       => ToTdbRow (TdbConsRow a b) where
  toTdbRow (a :. b) = toTdbField a:toTdbRow b
  {-# INLINE toTdbRow #-}

-- | Convenience newtype to put things that you can `Show` in TrailDB. It
-- implements `ToTdbRowField`.
newtype TdbShowable a = TdbShowable a
  deriving ( Functor, Foldable, Traversable, Typeable, Generic, Eq, Ord, Show, Read )

-- | Short-cut pattern synonym for `TdbShowable`
pattern TShow a = TdbShowable a

instance Show a => ToTdbRowField (TdbShowable a) where
  toTdbField (TdbShowable thing) = toTdbField $ show thing
  {-# INLINE toTdbField #-}

-- | Add a cookie with timestamp and values to `TdbCons`.
addTrail :: (MonadIO m, ToTdbRow r)
         => TdbCons
         -> UUID
         -> UnixTime
         -> r
         -> m ()
addTrail _ cookie _ _ | B.length cookie /= 16 =
  error "addTrail: cookie must be 16 bytes in length."
addTrail (TdbCons mvar) cookie epoch (toTdbRow -> values) = liftIO $ withMVar mvar $ \case
  Nothing -> error "addTrail: tdb_cons is closed."
  Just ptr ->
    B.unsafeUseAsCString cookie $ \cookie_ptr ->
      withBytestrings values $ \values_ptr ->
       withArray (fmap (fromIntegral . B.length) values) $ \values_length_ptr ->
        tdbThrowIfError $ tdb_cons_add
                            ptr
                            (castPtr cookie_ptr)
                            epoch
                            values_ptr
                            values_length_ptr
{-# INLINE addTrail #-}

-- | Finalizes a `TdbCons`.
--
-- You usually don't need to call this manually because this is called
-- automatically by `closeTrailDBCons` before actually closing `TdbCons`.
finalizeTrailDBCons :: MonadIO m => TdbCons -> m ()
finalizeTrailDBCons (TdbCons mvar) = liftIO $ withMVar mvar $ \case
  Nothing -> error "finalizeTrailDBCons: tdb_cons is closed."
  Just ptr -> do
    result <- tdb_cons_finalize ptr 0
    unless (result == 0) $ throwM FinalizationFailure

-- | Close a `TdbCons`
--
-- Does nothing if it's closed already.
closeTrailDBCons :: MonadIO m => TdbCons -> m ()
closeTrailDBCons (TdbCons mvar) = liftIO $ mask_ $ modifyMVar_ mvar $ \case
  Nothing -> return Nothing
  Just ptr -> do
    result <- tdb_cons_finalize ptr 0
    unless (result == 0) $ throwM FinalizationFailure
    tdb_cons_close ptr >> return Nothing

-- | Appends a `Tdb` to an open `TdbCons`.
appendTdbToTdbCons :: MonadIO m
                   => Tdb
                   -> TdbCons
                   -> m ()
appendTdbToTdbCons (Tdb mvar_tdb) (TdbCons mvar_tdb_cons) = liftIO $
  withMVar mvar_tdb_cons $ \case
    Nothing -> error "appendTdbToTdbCons: tdb_cons is closed."
    Just tdb_cons_ptr -> withCVar mvar_tdb $ \case
      Nothing -> error "appendTdbToTdbCons: tdb is closed."
      Just (tdbPtr -> tdb_ptr) -> do
        result <- tdb_cons_append tdb_cons_ptr tdb_ptr
        unless (result == 0) $
          error "appendTdbToTdbCons: tdb_cons_append() failed."

-- | Opens an existing TrailDB.
--
-- It can open file TrailDBs and also directory TrailDBs.
--
-- In case of files you can use format \"traildb.tdb\" or just \"traildb\".
openTrailDB :: MonadIO m
            => FilePath
            -> m Tdb
openTrailDB root = liftIO $ mask_ $
  withCString root $ \root_str -> do
    tdb <- tdb_init
    flip onException (when (tdb /= nullPtr) $ tdb_close tdb) $ do
      when (tdb == nullPtr) $ throwM CannotAllocateTrailDB

      tdbThrowIfError $ tdb_open tdb root_str

      buf <- mallocForeignPtrArray 1

      -- Protect concurrent access and attach a tdb_close to garbage collector
      mvar <- newCVar (Just TdbState {
          tdbPtr = tdb
        , decodeBuffer = buf
        , decodeBufferSize = 1
        })

      void $ mkWeakCVar mvar $ modifyCVar_ mvar $ \case
        Nothing -> return Nothing
        Just (tdbPtr -> ptr) -> tdb_close ptr >> return Nothing

      return $ Tdb mvar

-- | Hints that `Tdb` will not be accessed in near future.
--
-- Internally may invoke system call \'madvise\' behind the scenes to operating system.
--
-- This has no effect on semantics, only performance.
dontneedTrailDB :: MonadIO m
                => Tdb
                -> m ()
dontneedTrailDB tdb = withTdb tdb "dontneedTrailDB" tdb_dontneed

-- | Hints that `Tdb` will be walked over in near future.
--
-- Internally may invoke system call \'madvise\' behind the scenes to operating system.
--
-- This has no effect on semantics, only performance.
willneedTrailDB :: MonadIO m
                => Tdb
                -> m ()
willneedTrailDB tdb = withTdb tdb "willneedTrailDB" tdb_willneed

-- | Closes a TrailDB.
--
-- Does nothing if `Tdb` is already closed.
closeTrailDB :: MonadIO m
             => Tdb
             -> m ()
closeTrailDB (Tdb mvar) = liftIO $ mask_ $ modifyCVar_ mvar $ \case
  Nothing -> return Nothing
  Just (tdbPtr -> ptr) -> tdb_close ptr >> return Nothing

-- | Cursors are used to read a TrailDB.
--
-- It's permissible to make more than one cursor and use them on the same
-- `Tdb` concurrently from many threads.
--
-- However, you should not use the same cursor across threads.
--
-- Most operations on `Tdb` lock it so it cannot be used concurrenly. The
-- operations `setCursor`, `stepCursor` and `stepCursorList` don't lock anything.
data Cursor = Cursor {-# UNPACK #-} !(Ptr TdbCursorRaw)
                     !(IORef ())
                     !(MVar (Maybe TdbState))
  deriving ( Eq, Typeable, Generic )

-- | Creates a cursor to a trailDB.
makeCursor :: MonadIO m
           => Tdb
           -> m Cursor
makeCursor (Tdb mvar) = liftIO $ withCVar mvar $ \case
  Nothing -> error "makeCursor: tdb is closed."
  Just (tdbPtr -> tdb_ptr) -> mask_ $ do
    cursor <- tdb_cursor_new tdb_ptr
    cursor_fin <- newIORef ()
    void $ mkWeakIORef cursor_fin $ tdb_cursor_free cursor
    return $ Cursor cursor cursor_fin mvar
{-# NOINLINE makeCursor #-}

-- | Class of things that can be result from `getTrail`.
class FromTrail a where
  -- | Makes a result from a list of trails.
  --
  -- One crumb of a trail has timestamp and associative list of fields.
  --
  -- @
  --     [(timestamp, [(fieldname1, value1), (fieldname2, value2), ...]
  --     , ... ]
  -- @
  fromBytestringList :: [(UnixTime, [(B.ByteString, B.ByteString)])] -> a

instance FromTrail [(UnixTime, [(B.ByteString, B.ByteString)])] where
  fromBytestringList = id
  {-# INLINE fromBytestringList #-}

-- | Throws away timestamps.
instance FromTrail [M.Map B.ByteString B.ByteString] where
  fromBytestringList = fmap (M.fromList . snd)

-- | Throws away timestamps and field names.
instance FromTrail [[B.ByteString]] where
  fromBytestringList = fmap (fmap snd . snd)

-- | `Vector` version.
instance FromTrail (VS.Vector (M.Map B.ByteString B.ByteString)) where
  fromBytestringList = VS.fromList . fmap (M.fromList . snd)

-- | Set of all values that appear in the trail.
instance FromTrail (S.Set B.ByteString) where
  fromBytestringList = S.fromList . concatMap (fmap snd . snd)

-- | Convenience function that runs a function for each `TrailID` in TrailDB.
forEachTrailID :: (Applicative m, MonadIO m) => Tdb -> (TrailID -> m ()) -> m ()
forEachTrailID tdb action = do
  num_trails <- getNumTrails tdb
  for_ [0..num_trails-1] $ \tid -> action tid
{-# INLINEABLE forEachTrailID #-}

-- | Same as `forEachTrailID` but passes UUID as well.
forEachTrailIDUUID :: (Applicative m, MonadIO m) => Tdb -> (TrailID -> UUID -> m ()) -> m ()
forEachTrailIDUUID tdb action = do
  num_trails <- getNumTrails tdb
  for_ [0..num_trails-1] $ \tid -> do
    uuid <- getUUID tdb tid
    action tid uuid

-- | Same as `forEachTrailID` but arguments flipped.
traverseEachTrailID :: (Applicative m, MonadIO m) => (TrailID -> m ()) -> Tdb -> m ()
traverseEachTrailID action tdb = forEachTrailID tdb action
{-# INLINE traverseEachTrailID #-}

-- | Same as `traverseEachTrailID` but passes UUID as well.
traverseEachTrailIDUUID :: (Applicative m, MonadIO m) => (TrailID -> UUID -> m ()) -> Tdb -> m ()
traverseEachTrailIDUUID action tdb = forEachTrailIDUUID tdb action
{-# INLINE traverseEachTrailIDUUID #-}

-- | Fold TrailDB for each `TrailID`.
--
-- This is like `traverseEachTrailID` but lets you carry a folding value.
foldTrailDB :: MonadIO m => (a -> TrailID -> m a) -> a -> Tdb -> m a
foldTrailDB action initial tdb = do
  num_trails <- getNumTrails tdb
  foldlM action initial [0..num_trails-1]
{-# INLINEABLE foldTrailDB #-}

-- | Same as `foldTrailDB` but passes UUID as well.
foldTrailDBUUID :: MonadIO m => (a -> TrailID -> UUID -> m a) -> a -> Tdb -> m a
foldTrailDBUUID action initial tdb = do
  num_trails <- getNumTrails tdb
  foldlM (\accum tid -> do
           uuid <- getUUID tdb tid
           action accum tid uuid)
         initial [0..num_trails-1]
{-# INLINEABLE foldTrailDBUUID #-}

-- | Convenience function that returns a full trail in human-readable format.
--
-- This is quite a bit slower than using a cursor (`makeCursor`, `setCursor`,
-- `stepCursor`) but is simpler. If you don't need to go through bazillions of
-- data really fast you might want to use this one.
--
-- See `FromTrail` for things that can be taken as a result.
getTrail :: (FromTrail a, MonadIO m)
         => Tdb
         -> TrailID
         -> m a
getTrail tdb tid = liftIO $ do
  cursor <- makeCursor tdb
  setCursor cursor tid
  fromBytestringList <$> exhaustCursor cursor
 where
  exhaustCursor :: Cursor -> IO [(UnixTime, [(B.ByteString, B.ByteString)])]
  exhaustCursor cursor =
    stepCursor cursor >>= \case
      Nothing -> return []
      Just (unixtime, V.toList -> features) -> do
        let field_ids = features <&> (^.field)
        fieldnames <- for field_ids $ getFieldName tdb
        valuenames <- for features $ getValue tdb
        ((unixtime, zip fieldnames valuenames):) <$> exhaustCursor cursor

  (<&>) = flip (<$>)
{-# INLINEABLE getTrail #-}

-- | Same as `getTrail` but not polymorphic in output.
--
-- Meant to be used for very quick throw-away programs where you don't want to
-- spell out the type to be used (e.g. if you all you do is `print` the trail).
getTrailBytestring :: MonadIO m => Tdb -> TrailID -> m [(UnixTime, [(B.ByteString, B.ByteString)])]
getTrailBytestring = getTrail
{-# INLINE getTrailBytestring #-}

-- | Steps cursor forward in its trail.
--
-- Returns `Nothing` if there are no more crumbs in the trail.
stepCursor :: MonadIO m
           => Cursor
           -> m (Maybe Crumb)
stepCursor (Cursor cursor mvar finalizer) = liftIO $ do
  event_ptr <- shim_tdb_cursor_next cursor
  if event_ptr /= nullPtr
    then do let word64_ptr = castPtr event_ptr :: Ptr Word64
            timestamp <- peekElemOff word64_ptr 0
            num_items <- peekElemOff word64_ptr 1
            let items = plusPtr word64_ptr (2*sizeOf (undefined :: Word64))
              
            vec <- V.generateM (fromIntegral num_items) $ peekElemOff items

            touch mvar
            touch finalizer

            return $ Just (timestamp, vec)

    else return Nothing
{-# INLINE stepCursor #-}

-- | Same as `stepCursor` but returns the remaining trails on the cursor as a list.
--
-- Available since 0.1.1.0
stepCursorList :: MonadIO m
               => Cursor
               -> m [Crumb]
stepCursorList cursor = liftIO step_loop
 where
  step_loop = stepCursor cursor >>= \case
    Just item -> (item:) <$> step_loop
    _ -> return []
{-# INLINE stepCursorList #-}

-- | Puts cursor at the start of some trail.
setCursor :: MonadIO m
          => Cursor
          -> TrailID
          -> m ()
setCursor (Cursor cursor mvar finalizer) trail_id = liftIO $ do
  tdbThrowIfError $ tdb_get_trail cursor trail_id
  touch mvar
  touch finalizer
{-# INLINE setCursor #-}

withTdb :: MonadIO m => Tdb -> String -> (Ptr TdbRaw -> IO a) -> m a
withTdb (Tdb mvar) errstring action = liftIO $ withCVar mvar $ \case
  Nothing -> error $ errstring <> ": tdb is closed."
  Just (tdbPtr -> ptr) -> action ptr
{-# INLINE withTdb #-}

-- | Finds a uuid by trail ID.
getUUID :: MonadIO m => Tdb -> TrailID -> m UUID
getUUID tdb cid = withTdb tdb "getUUID" $ \ptr -> do
  cptr <- tdb_get_uuid ptr cid
  when (cptr == nullPtr) $
    throwM NoSuchTrailID
  B.packCStringLen (castPtr cptr, 16)
{-# INLINE getUUID #-}

-- | Finds a trail ID by uuid.
getTrailID :: MonadIO m => Tdb -> UUID -> m TrailID
getTrailID _ cookie | B.length cookie /= 16 = error "getTrailID: cookie must be 16 bytes in length."
getTrailID tdb cookie = withTdb tdb "getTrailID" $ \ptr ->
  B.unsafeUseAsCString cookie $ \cookie_str ->
    alloca $ \result_ptr -> do
      result <- tdb_get_trail_id ptr (castPtr cookie_str) result_ptr
      if result == 0
        then peek result_ptr
        else throwM NoSuchUUID
{-# INLINE getTrailID #-}

-- | Returns the number of cookies in `Tdb`
getNumTrails :: MonadIO m => Tdb -> m Word64
getNumTrails tdb = withTdb tdb "getNumTrails" tdb_num_trails

-- | Returns the number of events in `Tdb`
getNumEvents :: MonadIO m => Tdb -> m Word64
getNumEvents tdb = withTdb tdb "getNumEvents" tdb_num_events

-- | Returns the number of fields in `Tdb`
getNumFields :: MonadIO m => Tdb -> m Word64
getNumFields tdb = withTdb tdb "getNumFields" tdb_num_fields

-- | Returns the minimum timestamp in `Tdb`
getMinTimestamp :: MonadIO m => Tdb -> m UnixTime
getMinTimestamp tdb = withTdb tdb "getMinTimestamp" tdb_min_timestamp

-- | Returns the maximum timestamp in `Tdb`
getMaxTimestamp :: MonadIO m => Tdb -> m UnixTime
getMaxTimestamp tdb = withTdb tdb "getMaxTimestamp" tdb_max_timestamp

-- | Given a field ID, returns its human-readable field name.
getFieldName :: MonadIO m => Tdb -> FieldID -> m FieldName
getFieldName tdb fid = withTdb tdb "getFieldName" $ \ptr -> do
  result <- tdb_get_field_name ptr fid
  when (result == nullPtr) $ throwM NoSuchFieldID
  B.packCString result

-- | Given a field name, returns its `FieldID`.
getFieldID :: (FieldNameLike a, MonadIO m) => Tdb -> a -> m FieldID
getFieldID tdb (encodeToFieldName -> field_name) = withTdb tdb "getFieldID" $ \ptr ->
  B.useAsCString field_name $ \field_name_cstr ->
    alloca $ \field_ptr -> do
      tdbThrowIfError $ tdb_get_field ptr field_name_cstr field_ptr
      result <- peek field_ptr
      return $ fromIntegral $ result-1

-- | Given a `Feature`, returns a string that describes it.
--
-- Values in a TrailDB are integers which need to be mapped back to strings to
-- be human-readable.
getValue :: MonadIO m => Tdb -> Feature -> m B.ByteString
getValue tdb (Feature ft) = withTdb tdb "getValue" $ \ptr ->
  alloca $ \len_ptr -> do
    cstr <- tdb_get_item_value ptr ft len_ptr
    when (cstr == nullPtr) $ throwM NoSuchValue
    len <- peek len_ptr
    B.packCStringLen (cstr, fromIntegral len)
{-# INLINE getValue #-}

-- | Given a field ID and a human-readable value, turn it into `Feature` for that field ID.
getItem :: MonadIO m => Tdb -> FieldID -> B.ByteString -> m Feature
getItem tdb fid bs = withTdb tdb "getItem" $ \ptr ->
  B.unsafeUseAsCStringLen bs $ \(cstr, len) -> do
    ft <- tdb_get_item ptr (fid+1) cstr (fromIntegral len)
    if ft == 0
      then throwM NoSuchFeature
      else return $ Feature ft

-- | Same as `getItem` but uses a resolved field name rather than raw `FieldID`.
--
-- This is implemented in terms of `getFieldID` and `getItem` inside.
getItemByField :: (FieldNameLike a, MonadIO m) => Tdb -> a -> B.ByteString -> m Feature
getItemByField tdb (encodeToFieldName -> fid) bs = liftIO $ do
  fid <- getFieldID tdb fid
  getItem tdb fid bs

-- | Returns the raw pointer to a TrailDB.
--
-- You can pass this pointer to C code and use the C API of TrailDB to use it.
--
-- You become responsible for ensuring Haskell doesn't clean up and close the
-- managed `Tdb` handle. You can use `touchTdb` or `withRawTdb` to deal with this.
getRawTdb :: MonadIO m => Tdb -> m (Ptr TdbRaw)
getRawTdb (Tdb cvar) = liftIO $ withCVar cvar $ \case
  Nothing -> error "getRawTdb: tdb is closed."
  Just tdbstate -> return (tdbPtr tdbstate)

-- | Returns the raw pointer to a TrailDB construction handle.
--
-- Just as with `getRawTdb`, this pointer can be passed to C and used with the
-- TrailDB C API.
--
-- Use `touchTdbCons` or `withRawTdbCons` to ensure the pointer is not garbage
-- collected while you are using it.
getRawTdbCons :: MonadIO m => TdbCons -> m (Ptr TdbConsRaw)
getRawTdbCons (TdbCons cvar) = liftIO $ withCVar cvar $ \case
  Nothing -> error "getRawTdbCons: tdb_cons is closed."
  Just raw_ptr -> return raw_ptr

-- | Touch a `TdbCons`.
--
-- Ensures that `TdbCons` has not been garbage collected at the point
-- `touchTdbCons` is invoked. Has no other effect.
touchTdbCons :: MonadIO m => TdbCons -> m ()
touchTdbCons (TdbCons cvar) = liftIO $ withCVar cvar $ \case
  Nothing -> return ()
  Just raw_ptr -> touch raw_ptr

-- | Run an action with a raw pointer to `TdbCons`.
--
-- The `TdbCons` is guaranteed not to be garbage collected while the given
-- action is running.
withRawTdbCons :: MonadIO m => TdbCons -> (Ptr TdbConsRaw -> IO a) -> m a
withRawTdbCons tdb_cons action = do
  ptr <- getRawTdbCons tdb_cons
  liftIO $ finally (action ptr) (touch ptr)

-- | Touch a `Tdb`.
--
-- Ensures that `Tdb` has not been garbage collected at the point `touchTdb` is
-- invoked. Has no other effect.
touchTdb :: MonadIO m => Tdb -> m ()
touchTdb (Tdb cvar) = liftIO $ void $ withCVar cvar $ \case
  Nothing -> return ()
  Just tdbstate -> touch (tdbPtr tdbstate)

-- | Run an action with a raw pointer to `Tdb`.
--
-- The `Tdb` is guaranteed not to be garbage collected while the given action is running.
withRawTdb :: MonadIO m => Tdb -> (Ptr TdbRaw -> IO a) -> m a
withRawTdb tdb action = do
  ptr <- getRawTdb tdb
  liftIO $ finally (action ptr) (touchTdb tdb) 

-- | Opens a `Tdb` and then closes it after action is over.
withTrailDB :: (MonadIO m, MonadMask m) => FilePath -> (Tdb -> m a) -> m a
withTrailDB fpath action = mask $ \restore -> do
  tdb <- openTrailDB fpath
  finally (restore $ action tdb) (closeTrailDB tdb)
{-# INLINE withTrailDB #-}

-- | Given a directory, find all valid TrailDB paths inside it, recursively.
findTrailDBs :: forall m. (MonadIO m, MonadMask m)
             => FilePath
             -> Bool            -- ^ Follow symbolic links?
             -> m [FilePath]
findTrailDBs filepath follow_symbolic_links = do
  contents <- liftIO $ getDirectoryContents filepath
  dirs <- execStateT (filterChildDirectories filepath contents) [filepath]
  filterTrailDBDirectories dirs
 where
  filterChildDirectories :: FilePath -> [FilePath] -> StateT [FilePath] m ()
  filterChildDirectories prefix (".":rest) = filterChildDirectories prefix rest
  filterChildDirectories prefix ("..":rest) = filterChildDirectories prefix rest
  filterChildDirectories prefix (dir_raw:rest) = do
    let dir = prefix <> "/" <> dir_raw
    is_dir <- liftIO $ doesDirectoryExist dir
    is_symbolic_link_maybe <- liftIO $ tryIOError $ getFileStatus (T.encodeUtf8 $ T.pack dir)
    case is_symbolic_link_maybe of
      Left exc | isDoesNotExistError exc -> filterChildDirectories prefix rest
      Left exc -> throwM exc
      Right is_symbolic_link ->
        if is_dir && ((isSymbolicLink is_symbolic_link && follow_symbolic_links) ||
                      not (isSymbolicLink is_symbolic_link))
          then modify (dir:) >> recurse dir >> filterChildDirectories prefix rest
          else filterChildDirectories prefix rest
  filterChildDirectories _ [] = return ()

  recurse dir = do
    contents <- liftIO $ getDirectoryContents dir
    filterChildDirectories dir contents

-- | Given a list of directories, filters it, returning only directories that
-- are valid TrailDB directories.
--
-- Used internally by `findTrailDBs` but can be useful in general so we export it.
filterTrailDBDirectories :: (MonadIO m, MonadMask m) => [FilePath] -> m [FilePath]
filterTrailDBDirectories = filterM $ \dir -> do
  result <- try $ openTrailDB dir
  case result of
    Left CannotAllocateTrailDB -> return False
    Left exc -> throwM exc
    Right ok -> closeTrailDB ok >> return True

getTdbVersion :: MonadIO m => Tdb -> m TdbVersion
getTdbVersion tdb = withTdb tdb "getTdbVersion" tdb_version

