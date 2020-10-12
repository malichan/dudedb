package dudedb.storage

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject
import org.slf4j.LoggerFactory
import java.nio.file.Path

class Partition(
    val tokenRange: LongRange,
    dataDirPath: Path
) {
    private val logger = LoggerFactory.getLogger(Partition::class.java)
    private val dataFile = DataFile(dataDirPath.resolve("${tokenRange.toHexString()}.data"))
    private val indexFile = IndexFile(dataDirPath.resolve("${tokenRange.toHexString()}.index"))
    private val index = mutableMapOf<String, Long>()

    init {
        val indexFileEntries = indexFile.initialize()
        indexFileEntries.forEach { index[it.key] = it.offset }
        val dataFileOffsetOfLastEntry = indexFileEntries.lastOrNull()?.offset
        dataFile.initialize(dataFileOffsetOfLastEntry)
    }

    fun execPut(key: String, value: JsonObject) {
        logger.info("Partition[{}] PUT key={} value={}", tokenRange.toHexString(), key, value)
        val offset = dataFile.nextOffset
        dataFile.add(DataFileEntry(value.toString()))
        indexFile.add(IndexFileEntry(offset, key))
        index[key] = offset
    }

    fun execGet(key: String): JsonObject {
        logger.info("Partition[{}] GET key={}", tokenRange.toHexString(), key)
        return index[key]?.let {
            Json.parseToJsonElement(dataFile.load(it).value).jsonObject
        } ?: throw NoSuchElementException()
    }

    fun execList(limit: Int): Map<String, JsonObject> {
        logger.info("Partition[{}] LIST limit={}", tokenRange.toHexString(), limit)
        return index.asSequence().take(limit).map {
            it.key to Json.parseToJsonElement(dataFile.load(it.value).value).jsonObject
        }.toMap()
    }

    private fun LongRange.toHexString(): String =
        "${first.toString(16)}:${last.toString(16)}"
}
