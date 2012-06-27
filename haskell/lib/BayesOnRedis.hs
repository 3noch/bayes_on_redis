module BayesOnRedis where

import Control.Monad
import Data.ByteString.Char8 (ByteString, pack)
import qualified Data.ByteString.Char8 as B
import Database.Redis hiding (sort, sortBy)
import Data.Char
import Data.List
import Data.Maybe

type Category   = B.ByteString
type Document   = B.ByteString
type Word       = B.ByteString
type Tag        = B.ByteString

data Score = Score { scoreCategory   :: Category
                   , scoreClassifier :: Double
                   , scoreConfidence :: Double}
                   deriving (Show)


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


score :: Document -> IO [Maybe Score]
score doc = do
    conn   <- connect redisConfig
    cats   <- runRedis conn $ getMembersFromSet categoriesTag
    scores <- runRedis conn $ mapM (scoreInCategory words counts) cats
    return scores
    where (words, counts) = (unzip . countOccurrence) doc


classify :: Document -> IO (Maybe Category)
classify doc = do
    scores <- score doc
    return (classifyWithScores scores)


classifyWithScores :: [Maybe Score] -> Maybe Category
classifyWithScores scores
    | null usefulScores = Nothing
    | otherwise = Just $ (scoreCategory . last . sortBy compareScores) usefulScores
    where
        usefulScores = (map fromJust . filter isJust) scores
        compareScores s1 s2 = scoreClassifier s1 `compare` scoreClassifier s2


scoreInCategory :: [Word] -> [Integer] -> Category -> Redis (Maybe Score)
scoreInCategory words counts cat = do
    totalWords  <- either (const 0)  getDoubleOrZero `fmap` hget tag (pack ":total")
    redisCounts <- either (const []) (map getDoubleOrZero) `fmap` hmget tag words
    return $ if totalWords > 0
             then Just Score { scoreCategory   = cat
                             , scoreClassifier = 0 --classifier (scaleCounts redisCounts) totalWords
                             , scoreConfidence = confidence redisCounts }
             else Nothing
    where tag = getRedisCategoryTag cat

          scaleCounts redisCounts = zipWith (*) counts' redisCounts
              where counts' = map fromInteger counts

          classifier xs total = sum (map bayesFunc (positives xs))
              where bayesFunc x = log (x / total)

          confidence [] = 0
          confidence xs = genericLength (positives xs) / genericLength xs

          positives xs = filter (> 0) xs

          getDoubleOrZero :: Maybe B.ByteString -> Double
          getDoubleOrZero (Just str) = case B.readInt str of
              (Just (val, _)) -> fromIntegral val
              Nothing         -> 0.0
          getDoubleOrZero _          = 0.0


addCategory :: Category -> Redis ()
addCategory cat = sadd categoriesTag [cat] >> return ()


insertDocument :: Category -> Document -> Redis ()
insertDocument cat doc = do
    mapM_ (insertWord tag) wordCounts
    hincrby tag (pack ":total") total
    return ()
    where tag = getRedisCategoryTag cat
          insertWord tag (word, count) = hincrby tag word count >> return ()
          wordCounts = countOccurrence doc
          total = (sum . snd . unzip) wordCounts


removeDocument :: Category -> Document -> Redis ()
removeDocument cat doc = do
    mapM_ (removeWord tag) wordCounts
    hincrby tag (pack ":total") (-total)
    return ()
    where tag = getRedisCategoryTag cat
          wordCounts = countOccurrence doc
          total = (sum . snd . unzip) wordCounts
          removeWord tag (word, count) = do
              response <- hget tag word
              case readRedisInteger response of
                  Just old -> do -- TODO delete zero-keys
                      hset tag word (toByteString $ max 0 (old - count))
                      return ()
                  Nothing  -> return ()


countOccurrence :: Document -> [(Word, Integer)]
countOccurrence = map makeTuple . group . sort . splitDocument
    where makeTuple xs@(first:_) = (first, genericLength xs)
          splitDocument = filter (not . B.null) . B.splitWith (not . isAlphaNum)


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
getRedisCategoryTag cat = B.append (pack "BayesOnRedis:cat:") cat


getMembersFromSet :: Tag -> Redis [B.ByteString]
getMembersFromSet tag = do
    response <- smembers tag
    return $ case response of
        (Right members) -> members
        _               -> []
