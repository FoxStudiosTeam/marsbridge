package ru.foxstudios.marsbridge.service

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.udp.UdpClient

class EarthTransferService() {
    var client: Connection? = null
    val logger = LoggerFactory.getLogger(this::class.java)

    init {
        val factory = ConnectionFactory()

        factory.host = "mars-queue-service"
        factory.port = 5672
        val connection: com.rabbitmq.client.Connection = factory.newConnection()
        val channel = connection.createChannel()
        val deliverCallback: DeliverCallback = DeliverCallback { _, delivery ->
            val message = String(delivery.body, charset("UTF-8"))
            try {
                doWork(message)
            } finally {
                logger.info(" [x] Done - ok?!")
            }

        }
        channel.basicConsume("mars-queue", true, deliverCallback, { consumerTag -> })
        //client!!.onDispose().block()
    }

    fun doWork(message: String) {

        client = UdpClient.create().port(25577).host("host.docker.internal").wiretap(true).connectNow()
        logger.info(" [d] isDisposed true ${client!!.isDisposed}")

        val weight = runBlocking {
            countMessageWeight(message)
        }
        logger.info(" [x] Received '$message' weight: $weight")
        client!!.outbound().sendString(Mono.just(message)).then().subscribe()

        client!!.inbound().receive().asString().doOnTerminate {
            logger.info(
                "disconnect! ${client!!.isDisposed}, ${client!!.channel().isOpen}, ${client!!.channel().isActive}, ${
                    client!!.channel().remoteAddress()
                }"
            )
        }
            .doOnNext { text ->
                println(text)
                if (text == "ok") {
                    logger.info(" [x] Done! Remove $message from queue!")
                }
            }
            .doOnError { err -> logger.info(err.message); }
            .subscribe()
    }


    fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }
}
