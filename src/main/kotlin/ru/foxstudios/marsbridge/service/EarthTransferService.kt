package ru.foxstudios.marsbridge.service

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.udp.UdpClient
import java.awt.SystemColor.text
import java.nio.charset.StandardCharsets


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
                doWork(message, channel, delivery)
            } finally {
                logger.info(" [x] Done - ok?!")
            }

        }
        channel.basicConsume("mars-queue", false, deliverCallback, { consumerTag -> })
        //client!!.onDispose().block()
    }

    fun doWork(
        message: String, channel: Channel, delivery: Delivery
    ) {

        client = UdpClient.create().port(25577).host("host.docker.internal").wiretap(true).connectNow()
        logger.info(" [d] isDisposed true ${client!!.isDisposed}")

        val weight = runBlocking {
            countMessageWeight(message)
        }
        logger.info(" [x] Received '$message' weight: $weight")
        val list = ArrayList<String>()
        val length = message.length


        var i = 0
        while (i < length) {
            list.add(message.substring(i, Math.min(length, i + 8)))
            i += 8
        }
        for(elem in list){
            println(elem)
        }
        //.sendString(Mono.just(message))
        client!!.outbound().sendByteArray(Mono.just(message.toByteArray(StandardCharsets.UTF_8))).then().subscribe()

        client!!.inbound().receive().asString().doOnTerminate {
            logger.info(
                "disconnect! ${client!!.isDisposed}, ${client!!.channel().isOpen}, ${client!!.channel().isActive}, ${
                    client!!.channel().remoteAddress()
                }"
            )
        }
            .doOnNext { text ->
                logger.info(text)
                if (text == "ok") {
                    logger.info(" [x] Done! Remove $message from queue!")
                    channel.basicAck(delivery.envelope.deliveryTag, false)
                } else {
                    channel.basicNack(delivery.envelope.deliveryTag, false, true)
                }
            }
            .doOnError { err -> logger.info(err.message); }
            .subscribe()
    }


    fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }
}
