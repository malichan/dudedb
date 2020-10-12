package dudedb.message

import dudedb.message.MessageHandler.PORT
import org.slf4j.LoggerFactory
import org.zeromq.SocketType
import org.zeromq.ZContext
import java.net.InetAddress

object Messenger {
    private val logger = LoggerFactory.getLogger(Messenger::class.java)

    fun send(request: PutRequest, node: InetAddress): PutReply = doSend(request, node)

    fun send(request: GetRequest, node: InetAddress): GetReply = doSend(request, node)

    fun send(request: ListRequest, node: InetAddress): ListReply = doSend(request, node)

    @Suppress("UNCHECKED_CAST")
    private fun <I : Request, O : Reply> doSend(request: I, node: InetAddress): O =
        ZContext().use { context ->
            val socket = context.createSocket(SocketType.REQ)
            socket.connect("tcp://${node.hostAddress}:$PORT")
            val requestJsonString = request.toJsonString()
            logger.info("Sending Request {} to {}", requestJsonString, node.hostName)
            socket.send(requestJsonString)
            val replyJsonString = socket.recvStr()
            logger.info("Received Reply {} from {}", replyJsonString, node.hostName)
            Reply.fromJsonString(replyJsonString) as O
        }
}
