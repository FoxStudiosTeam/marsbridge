package ru.foxstudios.marsbridge.service

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import java.lang.instrument.Instrumentation


class EarthTransferService(var client: reactor.netty.Connection) {
    init {
        val factory = ConnectionFactory()
        val logger = LoggerFactory.getLogger(this::class.java)
        factory.host = "mars-queue-service"
        factory.port = 5672
        try {

            val connection: Connection = factory.newConnection()
            val channel = connection.createChannel()
            val deliverCallback: DeliverCallback = DeliverCallback { _, delivery ->
                val message = String(delivery.body, charset("UTF-8"))
                val weight = runBlocking {
                    countMessageWeight(message)
                }

                println(" [x] Received '$message' weight: $weight")
                client.outbound().sendString(Mono.just(message)).then().subscribe()
                client.inbound().receive().asString().doOnNext{text ->
                    logger.info(text)
                }
            }
            channel.basicConsume("mars-queue", false, deliverCallback, { consumerTag -> })
        } catch (e: Exception) {

        }
    }

    private suspend fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }

}
