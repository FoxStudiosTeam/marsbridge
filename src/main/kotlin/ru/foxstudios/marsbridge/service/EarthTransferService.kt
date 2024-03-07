package ru.foxstudios.marsbridge.service

import com.rabbitmq.client.Connection
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.runBlocking
import java.lang.instrument.Instrumentation


class EarthTransferService() {
    init {
        val factory = ConnectionFactory()
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
            }
            channel.basicConsume("mars-queue", true, deliverCallback, { consumerTag -> })
        } catch (e: Exception) {

        }
    }

    private suspend fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }

}
