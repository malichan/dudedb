package dudedb.query

import io.ktor.application.ApplicationCall
import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.http.HttpStatusCode
import io.ktor.request.receiveText
import io.ktor.response.respond
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.post
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.util.pipeline.PipelineContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import dudedb.message.GetReply
import dudedb.message.GetRequest
import dudedb.message.ListReply
import dudedb.message.ListRequest
import dudedb.message.Messenger
import dudedb.message.PutReply
import dudedb.message.PutRequest
import dudedb.node.NodeManager
import dudedb.storage.Partitioner
import org.slf4j.LoggerFactory

object QueryHandler {
    const val PORT = 8080

    private val logger = LoggerFactory.getLogger(QueryHandler::class.java)
    private val server = embeddedServer(
        Netty,
        port = PORT,
        host = "0.0.0.0"
    ) {
        routing {
            post("/data/{key}") {
                handlePut()
            }
            get("/data/{key}") {
                handleGet()
            }
            get("/data") {
                handleList()
            }
        }
    }

    fun start() {
        server.start()
    }

    private suspend fun PipelineContext<Unit, ApplicationCall>.handlePut() {
        logger.info("Handling PUT query")
        val request = try {
            val key = call.parameters["key"]!!
            val value = Json.parseToJsonElement(call.receiveText()).jsonObject
            PutRequest(key, value)
        } catch (e: Exception) {
            call.respond(HttpStatusCode.BadRequest)
            return
        }
        val token = Partitioner.getTokenForKey(request.key)
        val node = NodeManager.getNodeForToken(token)
        val reply = Messenger.send(request, node)
        when (reply) {
            is PutReply.Success ->
                call.respond(HttpStatusCode.OK)
            is PutReply.Failure ->
                when (reply.error) {
                    PutReply.Error.InternalServerError ->
                        call.respond(HttpStatusCode.InternalServerError)
                }
        }.apply {}
    }

    private suspend fun PipelineContext<Unit, ApplicationCall>.handleGet() {
        logger.info("Handling GET query")
        val request = try {
            val key = call.parameters["key"]!!
            GetRequest(key)
        } catch (e: Exception) {
            call.respond(HttpStatusCode.BadRequest)
            return
        }
        val token = Partitioner.getTokenForKey(request.key)
        val node = NodeManager.getNodeForToken(token)
        val reply = Messenger.send(request, node)
        when (reply) {
            is GetReply.Success ->
                call.respondText(ContentType.Application.Json, HttpStatusCode.OK) {
                    reply.value.toString()
                }
            is GetReply.Failure ->
                when (reply.error) {
                    GetReply.Error.NotFound ->
                        call.respond(HttpStatusCode.NotFound)
                    GetReply.Error.InternalServerError ->
                        call.respond(HttpStatusCode.InternalServerError)
                }
        }.apply {}
    }

    private suspend fun PipelineContext<Unit, ApplicationCall>.handleList() {
        logger.info("Handling LIST query")
        var limit = try {
            call.parameters["limit"]?.toInt() ?: Int.MAX_VALUE
        } catch (e: Exception) {
            call.respond(HttpStatusCode.BadRequest)
            return
        }
        val keyValuePairs = mutableMapOf<String, JsonObject>()
        for (node in NodeManager.getAllNodes().shuffled()) {
            if (limit <= 0) {
                break
            }
            val request = ListRequest(limit)
            val reply = Messenger.send(request, node)
            when (reply) {
                is ListReply.Success -> {
                    limit -= reply.keyValuePairs.size
                    keyValuePairs += reply.keyValuePairs
                }
                is ListReply.Failure -> {
                    when (reply.error) {
                        ListReply.Error.InternalServerError ->
                            call.respond(HttpStatusCode.InternalServerError)
                    }.apply {}
                    return
                }
            }.apply {}
        }
        call.respondText(ContentType.Application.Json, HttpStatusCode.OK) {
            keyValuePairs.entries.joinToString(
                separator = ",",
                prefix = "{",
                postfix = "}",
                transform = { (k, v) -> "\"$k\":$v" }
            )
        }
    }
}
