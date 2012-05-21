module Main where

import Control.Monad
import qualified Data.ByteString.Char8 as B
import Database.Redis hiding (sort)
import Data.Char
import Data.List

type Category = B.ByteString
type Document = B.ByteString
type Word     = B.ByteString
type Tag      = B.ByteString


redisConfig :: ConnectInfo
redisConfig = defaultConnectInfo
    { connectHost = "localhost"
    , connectPort = PortNumber 6379
    }


train :: Category -> Document -> IO ()
train cat doc = do
    conn <- connect redisConfig
    runRedis conn $ do
        addCategory cat
        insertDocument cat doc


untrain :: Category -> Document -> IO ()
untrain cat doc = do
    conn <- connect redisConfig
    runRedis conn $ removeDocument cat doc


addCategory :: Category -> Redis ()
addCategory cat = sadd getCategoriesTag [cat] >> return ()


applyDocumentWith :: (Tag -> (Word, Integer) -> Redis ())
                     -> Category
                     -> Document
                     -> Redis ()
applyDocumentWith modifier cat doc = mapM_ (modifier tag) (countOccurrence doc)
    where tag = getRedisCategoryTag cat


insertDocument :: Category -> Document -> Redis ()
insertDocument = applyDocumentWith insertWord
    where insertWord tag (word, count) = hincrby tag word count >> return ()


removeDocument :: Category -> Document -> Redis ()
removeDocument = applyDocumentWith removeWord
    where removeWord tag (word, count) = do
              response <- hget tag word
              case readRedisInteger response of
                  Just old -> do -- TODO delete zero-keys
                      hset tag word (integerToBs $ max 0 (old - count))
                      return ()
                  Nothing  -> return ()


countOccurrence :: Document -> [(Word, Integer)]
countOccurrence = map makeTuple . group . sort . splitDocument
    where makeTuple xs@(first:_) = (first, genericLength xs)
          splitDocument = B.splitWith (not . isAlphaNum)


readRedisInteger :: Either Reply (Maybe B.ByteString) -> Maybe Integer
readRedisInteger (Right (Just str)) =
    case B.readInteger str of
        (Just (val, _)) -> Just val
        _               -> Nothing
readRedisInteger _ = Nothing


integerToBs :: Integer -> B.ByteString
integerToBs = B.pack . show


getCategoriesTag :: Tag
getCategoriesTag = B.pack "BayesOnRedis:categories"


getRedisCategoryTag :: Category -> Tag
getRedisCategoryTag cat = B.append (B.pack "BayesOnRedis:cat:") cat'
    where cat' = B.map toLower cat


smembers_ :: Tag -> Redis [B.ByteString]
smembers_ tag = do
    response <- smembers tag
    return $ case response of
        (Right members) -> members
        _               -> []
