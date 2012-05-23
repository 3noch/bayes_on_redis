module Main where

import Control.Monad
import Data.ByteString.Char8 (ByteString, pack)
import qualified Data.ByteString.Char8 as B
import Database.Redis hiding (sort, sortBy)
import Data.Char
import Data.List

type Category = B.ByteString
type Document = B.ByteString
type Word     = B.ByteString
type Tag      = B.ByteString
type Score    = Double


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


score :: Document -> IO [(Category, Score)]
score doc = do
    conn   <- connect redisConfig
    cats   <- runRedis conn $ getMembersFromSet categoriesTag
    scores <- runRedis conn $ mapM (scoreInCategory words) cats
    return (zip cats scores)
    where (words, _) = unzip (countOccurrence doc)


classify :: Document -> IO Category
classify doc = do
    scores <- score doc
    return $ (fst . last . sortBy (\(_, s1) (_, s2) -> compare s1 s2)) scores


scoreInCategory :: [Word] -> Category -> Redis Score
scoreInCategory words cat = do
    totalWords' <- hvals tag
    let totalWords = case totalWords' of
                         (Right vals) -> sum $ map (getDoubleOrZero . Just) vals
                         _            -> 0
    redisCounts' <- hmget tag words
    let redisCounts = case redisCounts' of
                          (Right vals) -> map getDoubleOrZero vals
                          _            -> [] :: [Double]
    return $ sum $ map (\x -> log (x / totalWords)) (map (\x -> if x <= 0 then 0.1 else x) redisCounts)
    where tag = getRedisCategoryTag cat

          getDoubleOrZero :: Maybe B.ByteString -> Double
          getDoubleOrZero (Just str) = case B.readInt str of
              (Just (val, _)) -> fromIntegral val
              Nothing         -> 0.0
          getDoubleOrZero _          = 0.0


addCategory :: Category -> Redis ()
addCategory cat = sadd categoriesTag [cat] >> return ()


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
                      hset tag word (toByteString $ max 0 (old - count))
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


toByteString :: (Show a) => a -> B.ByteString
toByteString = pack . show


categoriesTag :: Tag
categoriesTag = pack "BayesOnRedis:categories"


getRedisCategoryTag :: Category -> Tag
getRedisCategoryTag cat = B.append (pack "BayesOnRedis:cat:") cat'
    where cat' = B.map toLower cat


getMembersFromSet :: Tag -> Redis [B.ByteString]
getMembersFromSet tag = do
    response <- smembers tag
    return $ case response of
        (Right members) -> members
        _               -> []
