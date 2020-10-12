package dudedb.message

import dudedb.storage.StorageManager
import org.slf4j.LoggerFactory
import org.zeromq.SocketType
import org.zeromq.ZContext
import kotlin.concurrent.thread

object MessageHandler {
    const val PORT = 5555

    private val logger = LoggerFactory.getLogger(MessageHandler::class.java)
    private val server = thread(start = false, name = "jeromqServer") {
        ZContext().use { context ->
            val socket = context.createSocket(SocketType.REP)
            socket.bind("tcp://*:$PORT")
            while (!Thread.currentThread().isInterrupted) {
                val request = Request.fromJsonString(socket.recvStr())
                val reply = handle(request)
                socket.send(reply.toJsonString())
            }
        }
    }

    fun start() {
        server.start()
    }

    private fun handle(request: Request): Reply =
        when (request) {
            is PutRequest -> handlePut(request)
            is GetRequest -> handleGet(request)
            is ListRequest -> handleList(request)
        }

    private fun handlePut(request: PutRequest): PutReply =
        try {
            logger.info("Handling PUT message")
            StorageManager.execPut(request.key, request.value)
            PutReply.Success
        } catch (e: Exception) {
            PutReply.Failure(PutReply.Error.InternalServerError)
        }

    private fun handleGet(request: GetRequest): GetReply =
        try {
            logger.info("Handling GET message")
            val value = StorageManager.execGet(request.key)
            GetReply.Success(value)
        } catch (e: Exception) {
            when (e) {
                is NoSuchElementException ->
                    GetReply.Failure(GetReply.Error.NotFound)
                else ->
                    GetReply.Failure(GetReply.Error.InternalServerError)
            }
        }

    private fun handleList(request: ListRequest): ListReply =
        try {
            logger.info("Handling LIST message")
            val keyValuePairs = StorageManager.execList(request.limit)
            ListReply.Success(keyValuePairs)
        } catch (e: Exception) {
            ListReply.Failure(ListReply.Error.InternalServerError)
        }
}
