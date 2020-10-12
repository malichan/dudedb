package dudedb.node

import java.math.BigInteger
import java.net.InetAddress
import java.util.TreeMap

object NodeManager {
    private val startingTokens = TreeMap<Long, InetAddress>().apply {
        val dockerComposeScale = System.getenv("DOCKER_COMPOSE_SCALE")?.toLong()
        if (dockerComposeScale == null) {
            put(Long.MIN_VALUE, InetAddress.getLocalHost())
        } else {
            val interval = ((Long.MAX_VALUE.toBigInteger() - Long.MIN_VALUE.toBigInteger() + BigInteger.ONE) /
                    dockerComposeScale.toBigInteger()).toLong()
            var startingToken = Long.MIN_VALUE
            for (i in 1..dockerComposeScale) {
                put(startingToken, InetAddress.getByName("dudedb_node_$i"))
                startingToken += interval
            }
        }
    }
    private val nodes = startingTokens.map { it.value to (it.key..it.key.next()) }
        .groupBy(Pair<InetAddress, LongRange>::first, Pair<InetAddress, LongRange>::second)

    fun getNodeForToken(token: Long): InetAddress = startingTokens.floorEntry(token).value

    fun getAllNodes(): Iterable<InetAddress> = nodes.keys

    fun getLocalNode(): InetAddress = InetAddress.getLocalHost().let { nodes.keys.first { node -> it == node } }

    fun getTokenRangesForNode(node: InetAddress): Iterable<LongRange> = nodes.getValue(node)

    private fun Long.next(): Long =
        startingTokens.ceilingKey(this + 1)?.let { it - 1 } ?: Long.MAX_VALUE
}
