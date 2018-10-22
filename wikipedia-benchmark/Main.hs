{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE LambdaCase #-}

module Main where

import qualified Data.Vector.Unboxed           as V
import           System.Environment
import           System.TrailDB

data EditIpCounts = EditIpCounts !Int !Int
  deriving ( Show )

main :: IO ()
main = do
  args <- getArgs
  case args of
    ["fast"  ] -> mainFast
    ["pretty"] -> mainPretty
    _ ->
      putStrLn "Please run benchmark with either 'fast' or 'pretty' argument."

mainPretty :: IO ()
mainPretty
  = withTrailDB "wikipedia-history-small.tdb"
    $ \traildb -> print =<< foldEachEvent
        traildb
        (\(EditIpCounts edits ip_edits) _ crumb -> do
          (_timestamp, fields) <- decodeCrumbBytestring traildb crumb
          pure $ EditIpCounts
            (edits + if any (\pair -> pair == ("user", "")) fields then 1 else 0
            )
            ( ip_edits
            + if any (\pair -> pair == ("ip", "")) fields then 1 else 0
            )
        )
        (EditIpCounts 0 0)

mainFast :: IO ()
mainFast = withTrailDB "wikipedia-history-small.tdb" $ \traildb -> do
  cursor     <- makeCursor traildb
  user_field <- fromIntegral <$> getFieldID traildb ("user" :: String)
  ip_field   <- fromIntegral <$> getFieldID traildb ("ip" :: String)
  empty_user <- getItem traildb user_field ""
  empty_ip   <- getItem traildb ip_field ""
  print
    =<< foldTrailDB
          (countEditsAndIPs cursor
                            (fromIntegral user_field)
                            (fromIntegral ip_field)
                            empty_user
                            empty_ip
          )
          (EditIpCounts 0 0)
          traildb
 where
  countEditsAndIPs
    :: Cursor
    -> Int
    -> Int
    -> Feature
    -> Feature
    -> EditIpCounts
    -> TrailID
    -> IO EditIpCounts
  countEditsAndIPs cursor user_field ip_field empty_user empty_ip counts trail_id
    = do
      setCursor cursor trail_id

      let go accum@(EditIpCounts edits ip_edits) = stepCursor cursor >>= \case
            Nothing -> pure accum
            Just (_, event_values) ->
              let ip_value   = event_values V.! ip_field
                  user_value = event_values V.! user_field
                  ip_add     = if ip_value == empty_ip then 1 else 0
                  user_add   = if user_value == empty_user then 1 else 0
              in  go $ EditIpCounts (edits + user_add) (ip_edits + ip_add)

      go counts
