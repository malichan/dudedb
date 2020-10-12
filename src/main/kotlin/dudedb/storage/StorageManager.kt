package dudedb.storage

import dudedb.node.NodeManager
import kotlinx.serialization.json.JsonObject
import java.nio.file.Files
import java.nio.file.Path
import java.util.TreeMap

object StorageManager {
    const val ROOT_DIR_NAME = "data"

    private val localNode = NodeManager.getLocalNode()
    private val dataDirPath = Path.of(ROOT_DIR_NAME, localNode.hostName).also { Files.createDirectories(it) }
    private val startingTokens = NodeManager.getTokenRangesForNode(localNode)
        .associateTo(TreeMap()) { it.first to Partition(it, dataDirPath) }

    fun execPut(key: String, value: JsonObject): Unit =
        getPartitionForKey(key)?.execPut(key, value) ?: throw IllegalStateException()

    fun execGet(key: String): JsonObject =
        getPartitionForKey(key)?.execGet(key) ?: throw IllegalStateException()

    @Suppress("NAME_SHADOWING")
    fun execList(limit: Int): Map<String, JsonObject> {
        var limit = limit
        val keyValuePairs = mutableMapOf<String, JsonObject>()
        for (partition in startingTokens.values.shuffled()) {
            if (limit <= 0) {
                break
            }
            partition.execList(limit).let {
                limit -= it.size
                keyValuePairs += it
            }
        }
        return keyValuePairs
    }

    private fun getPartitionForKey(key: String): Partition? {
        val token = Partitioner.getTokenForKey(key)
        return startingTokens.floorEntry(token)?.let {
            if (token in it.value.tokenRange) {
                it.value
            } else {
                null
            }
        }
    }
}
