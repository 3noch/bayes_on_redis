module Main where

import Control.Monad
import qualified Data.ByteString.Char8 as Bs
import qualified Database.Redis as Rs
import Data.List

type Category = Bs.ByteString
type Document = Bs.ByteString
type Word     = Bs.ByteString
type Tag      = Bs.ByteString


redisConfig :: Rs.ConnectInfo
redisConfig = Rs.defaultConnectInfo
    { Rs.connectHost = "localhost"
    , Rs.connectPort = Rs.PortNumber 6379
    }


train :: Category -> Document -> IO ()
train cat doc = do
    conn <- Rs.connect redisConfig
    Rs.runRedis conn $ do
        addCategory cat
        insertDocument cat doc


untrain :: Category -> Document -> IO ()
untrain cat doc = do
    conn <- Rs.connect redisConfig
    Rs.runRedis conn $ removeDocument cat doc


addCategory :: Category -> Rs.Redis ()
addCategory cat = do
    Rs.sadd getCategoriesTag [cat]
    return ()


applyDocumentWith :: (Tag -> (Word, Integer) -> Rs.Redis ())
                     -> Category
                     -> Document
                     -> Rs.Redis ()
applyDocumentWith modifier cat doc = mapM_ (modifier tag) (countOccurrence doc)
    where tag = getRedisCategoryTag cat


insertDocument :: Category -> Document -> Rs.Redis ()
insertDocument = applyDocumentWith insertWord
    where
        insertWord :: Tag -> (Word, Integer) -> Rs.Redis ()
        insertWord tag (word, count) = do
            Rs.hincrby tag word count
            return ()


removeDocument :: Category -> Document -> Rs.Redis ()
removeDocument = applyDocumentWith removeWord
    where
        removeWord :: Tag -> (Word, Integer) -> Rs.Redis ()
        removeWord tag (word, count) = do
            response <- Rs.hget tag word
            case readRedisInteger response of
                Just old -> do
                    Rs.hset tag word (integerToBs $ max 0 (old - count))
                    return ()
                Nothing       -> return ()


countOccurrence :: Document -> [(Word, Integer)]
countOccurrence = countWordOccurrence . splitDocument


countWordOccurrence :: [Word] -> [(Word, Integer)]
countWordOccurrence = map makeTuple . group . sort
    where
        makeTuple :: [Word] -> (Word, Integer)
        makeTuple xs@(first:_) = (first, genericLength xs)


splitDocument :: Document -> [Word]
splitDocument = Bs.splitWith (not . isAlphaNum)


alphanums :: String
alphanums = ['a'..'z'] ++ ['A'..'Z'] ++ ['0'..'9']


isAlphaNum :: Char -> Bool
isAlphaNum = (`elem` alphanums)


readRedisInteger :: Either Rs.Reply (Maybe Bs.ByteString) -> Maybe Integer
readRedisInteger (Right (Just str)) =
    case Bs.readInteger str of
        (Just (val, _)) -> Just val
        Nothing         -> Nothing
readRedisInteger _ = Nothing


integerToBs :: Integer -> Bs.ByteString
integerToBs = Bs.pack . show


getCategoriesTag :: Tag
getCategoriesTag = Bs.pack "BayesOnRedis:categories"


getRedisCategoryTag :: Category -> Tag
getRedisCategoryTag = Bs.append (Bs.pack "BayesOnRedis:cat:")
