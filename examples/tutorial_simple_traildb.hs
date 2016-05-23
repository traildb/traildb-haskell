module Main ( main ) where

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL
import Data.Foldable
import Data.UUID
import Data.UUID.V4
import Data.Time
import System.TrailDB

main :: IO ()
main = do
  withTrailDBCons "tiny" ["username", "action"] $ \cons ->
    for_ [0..2] $ \i -> do
      uuid <- BL.toStrict . toByteString <$> nextRandom
      let username = "user" ++ show i
      for_ (zip [0..] ["open", "save", "close"]) $ \(day, action) ->
        addTrail cons uuid
                 (dayToUnixTime (fromGregorian 2016 (1+i) (1+day)))
                 [username, action]

  withTrailDB "tiny" $ \tdb -> forEachTrailIDUUID tdb $ \tid uuid -> do
    trail <- getTrailBytestring tdb tid
    print (uuid, trail)

