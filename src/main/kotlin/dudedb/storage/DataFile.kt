package dudedb.storage

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.READ
import java.nio.file.StandardOpenOption.SYNC
import java.nio.file.StandardOpenOption.WRITE

class DataFile(path: Path) {
    private val fileChannel = FileChannel.open(path, CREATE, READ, WRITE, SYNC)
    var nextOffset = -1L
        private set

    fun initialize(offsetOfLastEntry: Long?) {
        check(nextOffset < 0)
        nextOffset = offsetOfLastEntry?.let {
            fileChannel.position(it)
            checkNotNull(DataFileEntry.readFrom(fileChannel))
            fileChannel.position()
        } ?: 0
    }

    fun add(entry: DataFileEntry) {
        check(nextOffset >= 0)
        fileChannel.position(nextOffset)
        entry.writeTo(fileChannel)
        nextOffset = fileChannel.position()
    }

    fun load(offset: Long): DataFileEntry {
        check(nextOffset >= 0)
        fileChannel.position(offset)
        return checkNotNull(DataFileEntry.readFrom(fileChannel))
    }
}

class DataFileEntry(
    private val valueBytes: ByteArray
) {
    constructor(value: String) : this(value.toByteArray(StandardCharsets.UTF_8))

    val value get() = String(valueBytes, StandardCharsets.UTF_8)
    private val size get() = HEADER_SIZE + valueBytes.size

    fun writeTo(fileChannel: FileChannel) {
        val byteBuffer = ByteBuffer.allocate(size)
        byteBuffer.putInt(valueBytes.size)
        byteBuffer.put(valueBytes)
        byteBuffer.flip()
        fileChannel.write(byteBuffer).also { check(it == size) }
    }

    companion object {
        const val HEADER_SIZE = Int.SIZE_BYTES

        fun readFrom(fileChannel: FileChannel): DataFileEntry? {
            val headerByteBuffer = ByteBuffer.allocate(HEADER_SIZE)
            fileChannel.read(headerByteBuffer).also {
                if (it >= 0) {
                    check(it == HEADER_SIZE)
                } else {
                    return null
                }
            }
            headerByteBuffer.flip()
            val bodySize = headerByteBuffer.int
            val bodyByteBuffer = ByteBuffer.allocate(bodySize)
            fileChannel.read(bodyByteBuffer).also { check(it == bodySize) }
            return DataFileEntry(bodyByteBuffer.array())
        }
    }
}
