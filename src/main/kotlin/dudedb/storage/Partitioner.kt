package dudedb.storage

import java.math.BigInteger
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Partitioner {
    fun getTokenForKey(key: String): Long =
        MessageDigest.getInstance("MD5")
            .digest(key.toByteArray(StandardCharsets.UTF_8))
            .let { BigInteger(it).toLong() }
}
