{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module System.TrailDB.Error
  ( TrailDBException(..) )
  where

import Control.Exception
import Data.Data
import Foreign.C.Types
import GHC.Generics

-- | Exceptions that may happen with TrailDBs.
--
-- Some programming errors may throw with `error` instead.
data TrailDBException
  = CannotAllocateTrailDBCons   -- ^ Failed to allocate `TdbCons`.
  | CannotAllocateTrailDB       -- ^ Failed to allocate `Tdb`.
  | TrailDBError !CInt String   -- ^ Errors reported by error code from TrailDB C library.
                                --   includes numerical error and human-readable error.
  | NoSuchTrailID               -- ^ A `UUIDID` was used that doesn't exist in `Tdb`.
  | NoSuchUUID                  -- ^ A `UUID` was used that doesn't exist in `Tdb`.
  | NoSuchFieldID               -- ^ A `FieldID` was used that doesn't exist in `Tdb`.
  | NoSuchField                 -- ^ A `Field` was used that doesn't exist in `Tdb`.
  | NoSuchValue                 -- ^ A `Feature` was used that doesn't contain a valid value.
  | NoSuchFeature               -- ^ Attempted to find `Feature` for human readable name that doesn't exist.
  | FinalizationFailure         -- ^ For some reason, finalizing a `TdbCons` failed.
  deriving ( Eq, Ord, Show, Read, Typeable, Generic )

instance Exception TrailDBException

