{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}

module System.TrailDB.Error
  ( TrailDBError(..) )
  where

import Control.Exception
import Data.Data
import Foreign.C.Types
import GHC.Generics

data TrailDBError = TrailDBError !CInt
  deriving ( Eq, Ord, Show, Read, Typeable, Generic )

instance Exception TrailDBError

