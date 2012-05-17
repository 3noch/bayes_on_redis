module Main where

import qualified Data.ByteString.Char8 as Bs
import Database.Redis

type Category = Bs.ByteString
type Document = Bs.ByteString
type Word     = Bs.ByteString

redisConfig :: ConnectInfo
redisConfig = defaultConnectInfo { connectHost = "localhost"
                                 , connectPort = PortNumber 6379
                                 }

categories_key = Bs.pack "BayesOnRedis:categories"

train :: Category -> Document -> IO ()
train category text = do
    conn <- connect redisConfig
    runRedis conn $ do
        sadd categories_key category
