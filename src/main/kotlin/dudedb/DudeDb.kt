package dudedb

import dudedb.message.MessageHandler
import dudedb.query.QueryHandler

fun main() {
    MessageHandler.start()
    QueryHandler.start()
}
