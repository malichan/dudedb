package dudedb.message

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject

@Serializable
sealed class Request {
    fun toJsonString(): String = Json.encodeToString(this)

    companion object {
        fun fromJsonString(jsonString: String): Request = Json.decodeFromString(jsonString)
    }
}

@Serializable
sealed class Reply {
    fun toJsonString(): String = Json.encodeToString(this)

    companion object {
        fun fromJsonString(jsonString: String): Reply = Json.decodeFromString(jsonString)
    }
}

@Serializable
@SerialName("Put")
data class PutRequest(val key: String, val value: JsonObject) : Request()

@Serializable
sealed class PutReply : Reply() {
    @Serializable
    @SerialName("Put.Success")
    object Success : PutReply()

    @Serializable
    @SerialName("Put.Failure")
    data class Failure(val error: Error) : PutReply()

    enum class Error {
        InternalServerError
    }
}

@Serializable
@SerialName("Get")
data class GetRequest(val key: String) : Request()

@Serializable
sealed class GetReply : Reply() {
    @Serializable
    @SerialName("Get.Success")
    data class Success(val value: JsonObject) : GetReply()

    @Serializable
    @SerialName("Get.Failure")
    data class Failure(val error: Error) : GetReply()

    enum class Error {
        NotFound,
        InternalServerError
    }
}

@Serializable
@SerialName("List")
data class ListRequest(val limit: Int) : Request()

@Serializable
sealed class ListReply : Reply() {
    @Serializable
    @SerialName("List.Success")
    data class Success(val keyValuePairs: Map<String, JsonObject>) : ListReply()

    @Serializable
    @SerialName("List.Failure")
    data class Failure(val error: Error) : ListReply()

    enum class Error {
        InternalServerError
    }
}
