package dudedb.storage

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.SYNC
import java.nio.file.StandardOpenOption.WRITE

class IndexFile(path: Path) {
    private val fileChannel = FileChannel.open(path, CREATE, READ, WRITE, SYNC)
    private var nextOffset = -1L

    fun initialize(): List<IndexFileEntry> {
        check(nextOffset < 0)
        val entries = mutableListOf<IndexFileEntry>()
        fileChannel.position(0)
        while (true) {
            IndexFileEntry.readFrom(fileChannel)?.let {
                entries.add(it)
            } ?: break
        }
        nextOffset = fileChannel.position()
        return entries
    }

    fun add(entry: IndexFileEntry) {
        check(nextOffset >= 0)
        fileChannel.position(nextOffset)
        entry.writeTo(fileChannel)
        nextOffset = fileChannel.position()
    }
}

class IndexFileEntry(
    val offset: Long,
    private val keyBytes: ByteArray
) {
    constructor(offset: Long, key: String) : this(offset, key.toByteArray(StandardCharsets.UTF_8))

    val key get() = String(keyBytes, StandardCharsets.UTF_8)
    private val size get() = HEADER_SIZE + keyBytes.size

    fun writeTo(fileChannel: FileChannel) {
        val byteBuffer = ByteBuffer.allocate(size)
        byteBuffer.putLong(offset)
        byteBuffer.putInt(keyBytes.size)
        byteBuffer.put(keyBytes)
        byteBuffer.flip()
        fileChannel.write(byteBuffer).also { check(it == size) }
    }

    companion object {
        const val HEADER_SIZE = Long.SIZE_BYTES + Int.SIZE_BYTES

        fun readFrom(fileChannel: FileChannel): IndexFileEntry? {
            val headerByteBuffer = ByteBuffer.allocate(HEADER_SIZE)
            fileChannel.read(headerByteBuffer).also {
                if (it >= 0) {
                    check(it == HEADER_SIZE)
                } else {
                    return null
                }
            }
            headerByteBuffer.flip()
            val offset = headerByteBuffer.long
            val bodySize = headerByteBuffer.int
            val bodyByteBuffer = ByteBuffer.allocate(bodySize)
            fileChannel.read(bodyByteBuffer).also { check(it == bodySize) }
            return IndexFileEntry(offset, bodyByteBuffer.array())
        }
    }
}
