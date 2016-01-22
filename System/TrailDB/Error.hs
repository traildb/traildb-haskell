{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module System.TrailDB.Error
  ( TrailDBError(..) )
  where

import Control.Exception
import Data.Data
import Foreign.C.Types
import GHC.Generics

-- | Most errors thrown from TrailDB are described by this data type.
data TrailDBError = TrailDBError
 !CInt    -- ^ The raw error code
  String  -- ^ Human-readable error string
  deriving ( Eq, Ord, Show, Read, Typeable, Generic )

instance Exception TrailDBError

