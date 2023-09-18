{-# LANGUAGE OverloadedStrings #-}

import Connection (Connection (..))
import Control.Concurrent (Chan, MVar, dupChan, forkIO, newChan, newMVar, putMVar, readChan, readMVar, takeMVar, threadDelay, writeChan)
import Control.Exception (AsyncException (..), Handler (..), SomeException (..), catches)
import Control.Monad qualified as Monad (forever, unless, when)
import Data.Aeson qualified as JSON (eitherDecode, encode)
import Data.Map.Strict as Map (Map, delete, empty, insert)
import Data.Set as Set (Set, empty, insert)
import Data.Time.Clock.POSIX (POSIXTime, getPOSIXTime)
import Data.UUID.V4 qualified as UUID (nextRandom)
import Message (Message (Message), addVisited, appendMessage, creator, messageId, metadata, payload, readMessages, setCreator, setFlow)
import MessageFlow (MessageFlow (..))
import MessageId (MessageId)
import Metadata (Metadata (..))
import Network.WebSockets (ConnectionException (..))
import Network.WebSockets qualified as WS (ClientApp, DataMessage (..), fromLazyByteString, receiveDataMessage, runClient, sendTextData)
import Options.Applicative qualified as Options
import Payload (Payload (..))
import Service (Service (..))
import System.Exit (exitSuccess)

-- dir, port, file
data Options = Options !FilePath !Host !Port

type Host = String
type Port = Int

data State = State
    { pending :: Map MessageId Message
    , uuids :: Set MessageId
    , syncing :: Bool
    }
    deriving (Show)
type StateMV = MVar State

myself :: Service
myself = Dumb

emptyState :: State
emptyState =
    State
        { pending = Map.empty
        , Main.uuids = Set.empty
        , syncing = True
        }

options :: Options.Parser Options
options =
    Options
        <$> Options.strOption
            ( Options.short 'f'
                <> Options.long "file"
                <> Options.value "data/messagestore.txt"
                <> Options.help "Filename of the file containing messages"
            )
        <*> Options.strOption
            ( Options.short 'h'
                <> Options.long "store_host"
                <> Options.value "localhost"
                <> Options.help "Hostname of the Store service. [default: localhost]"
            )
        <*> Options.option
            Options.auto
            ( Options.long "store_port"
                <> Options.metavar "STORE_PORT"
                <> Options.value 8081
                <> Options.help "Port of the Store service.  [default: 8081]"
            )

clientApp :: FilePath -> Chan Message -> StateMV -> WS.ClientApp ()
clientApp msgPath storeChan stateMV conn = do
    putStrLn "Connected!"
    -- Just reconnected, first send an InitiatedConnection to the store
    newUuid <- UUID.nextRandom
    currentTime <- getPOSIXTime
    state <- readMVar stateMV
    -- send an initiatedConnection
    let initiatedConnection =
            Message
                (Metadata{uuid = newUuid, Metadata.when = currentTime, Metadata.from = [myself], Metadata.flow = Requested})
                (InitiatedConnection (Connection{lastMessageTime = 0, Connection.uuids = Main.uuids state}))
    _ <- WS.sendTextData conn $ JSON.encode initiatedConnection
    -- Just reconnected, send the pending messages to the Store
    mapM_ (WS.sendTextData conn . JSON.encode . addVisited myself) (pending state)
    -- fork a thread to send back data from the channel to the central store
    -- CLIENT WORKER THREAD
    _ <- forkIO $ do
        Monad.forever $ do
            msg <- readChan storeChan -- here we get all messages from all browsers
            putStrLn $ "CLIENT WORKER THREAD received the msg from browsers:\n" ++ show msg
            case flow (metadata msg) of
                Requested -> case creator msg of
                    Front -> do
                        -- process
                        processedMsgs <- processMessage msg
                        st <- takeMVar stateMV
                        putMVar stateMV $! foldl update st processedMsgs
                        -- send to the Store
                        mapM_ (appendMessage msgPath) processedMsgs
                        mapM_ (WS.sendTextData conn . JSON.encode) processedMsgs
                        putStrLn $ "Sent back this msg to the store: " ++ show processedMsgs
                    _ -> return ()
                _ -> return ()

    -- CLIENT MAIN THREAD
    -- loop on the handling of messages incoming through websocket
    Monad.forever $ do
        message <- WS.receiveDataMessage conn
        putStrLn $ "CLIENT MAIN THREAD received the msg from browsers:\n" ++ show message
        case JSON.eitherDecode
            ( case message of
                WS.Text bs _ -> WS.fromLazyByteString bs
                WS.Binary bs -> WS.fromLazyByteString bs
            ) of
            Right msg -> do
                st' <- readMVar stateMV
                case flow (metadata msg) of
                    Requested -> case creator msg of
                        Front -> Monad.when (messageId msg `notElem` Main.uuids st') $ do
                            appendMessage msgPath msg
                            -- Add it or remove to the pending list (if relevant) and keep the uuid
                            st'' <- takeMVar stateMV
                            putMVar stateMV $! update st'' msg
                            putStrLn "updated state"
                            -- send msg to the worker thread and to other connected clients
                            Monad.unless (syncing st') $ do
                                putStrLn "Writing to the chan"
                                writeChan storeChan msg
                        _ -> return ()
                    Processed -> case payload msg of
                        InitiatedConnection _ -> do
                            st''' <- takeMVar stateMV
                            putMVar stateMV $! st'''{syncing = False}
                            putStrLn "Left the syncing mode"
                        _ -> Monad.when (messageId msg `notElem` Main.uuids st') $ do
                            appendMessage msgPath msg
                            -- Add it or remove to the pending list (if relevant) and keep the uuid
                            st'' <- takeMVar stateMV
                            putMVar stateMV $! update st'' msg
                            putStrLn "updated state"
                            -- send msg to the worker thread and to other connected clients
                            Monad.unless (syncing st') $ do
                                putStrLn "Writing to the chan"
                                writeChan storeChan msg
                    _ -> return ()
            Left err -> putStrLn $ "### ERROR ### decoding incoming message:\n" ++ err

update :: State -> Message -> State
update state msg =
    case flow (metadata msg) of
        Requested -> case payload msg of
            InitiatedConnection _ -> state
            _ ->
                state
                    { pending = Map.insert (messageId msg) msg $ pending state
                    , Main.uuids = Set.insert (messageId msg) (Main.uuids state)
                    }
        Processed ->
            state
                { pending = Map.delete (messageId msg) $ pending state
                , Main.uuids = Set.insert (messageId msg) (Main.uuids state)
                }
        Error _ -> state

processMessage :: Message -> IO [Message]
processMessage msg = do
    case payload msg of
        AddedIdentifier _ -> return []
        AddedIdentifierType _ -> return []
        RemovedIdentifierType _ -> return []
        ChangedIdentifierType _ _ -> return []
        _ -> return [setFlow Processed $ setCreator myself msg]

maxWait :: Int
maxWait = 10

reconnectClient :: Int -> POSIXTime -> Host -> Port -> FilePath -> Chan Message -> StateMV -> IO ()
reconnectClient waitTime previousTime host port msgPath storeChan stateMV = do
    putStrLn $ "Waiting " ++ show waitTime ++ " seconds"
    threadDelay $ waitTime * 1000000
    putStrLn $ "Connecting to Store at ws://" ++ host ++ ":" ++ show port ++ "..."

    catches
        (WS.runClient host port "/" (clientApp msgPath storeChan stateMV))
        [ Handler
            (\(_ :: ConnectionException) -> reconnectClient 1 previousTime host port msgPath storeChan stateMV)
        , Handler
            ( \(e :: AsyncException) -> case e of
                UserInterrupt -> do
                    putStrLn "Stopping..."
                    exitSuccess
                _ -> return ()
            )
        , Handler
            ( \(_ :: SomeException) ->
                do
                    disconnectTime <- getPOSIXTime
                    let newWaitTime = if fromEnum (disconnectTime - previousTime) >= (1000000000000 * (maxWait + 1)) then 1 else min maxWait $ waitTime + 1
                    reconnectClient newWaitTime disconnectTime host port msgPath storeChan stateMV
            )
        ]

serve :: Options -> IO ()
serve (Options msgPath storeHost storePort) = do
    chan <- newChan -- main channel, that will be duplicated for the store
    stateMV <- newMVar emptyState
    firstTime <- getPOSIXTime
    storeChan <- dupChan chan -- output channel to the central message store
    -- Reconstruct the state
    putStrLn "Reconstructing the State..."
    msgs <- readMessages msgPath
    state <- takeMVar stateMV
    let newState = foldl update state msgs -- TODO foldr or strict foldl ?
    putMVar stateMV newState
    putStrLn $ "STATE:\n" ++ show newState
    -- keep connection to the Store
    reconnectClient 1 firstTime storeHost storePort msgPath storeChan stateMV

main :: IO ()
main =
    serve =<< Options.execParser opts
  where
    opts =
        Options.info
            (options Options.<**> Options.helper)
            ( Options.fullDesc
                <> Options.progDesc "Dumb does nothing but returning a Processed msg"
                <> Options.header "Modelyz Dumb"
            )
