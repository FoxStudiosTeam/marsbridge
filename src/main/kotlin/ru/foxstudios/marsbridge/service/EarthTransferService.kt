package ru.foxstudios.marsbridge.service

import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import kotlinx.coroutines.runBlocking
import org.apache.commons.io.FileUtils
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.udp.UdpClient
import java.io.File
import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.Path


class EarthTransferService() {
    var client: Connection? = null

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
                println(" [x] Done - ok?!")
            }

        }
        channel.basicConsume("mars-queue", false, deliverCallback, { consumerTag -> })
        //client!!.onDispose().block()
    }

    fun doWork(
        message: String, channel: Channel, delivery: Delivery
    ) {

        client = UdpClient.create().port(25577).host("host.docker.internal").wiretap(true).connectNow()


        val weight = runBlocking {
            countMessageWeight(message)
        }
        println(" [x] Received '$message' weight: $weight")
        client!!.outbound().sendObject(Mono.just(message)).then().subscribe()

        client!!.inbound().receive().asString().doOnTerminate {
            println(
                "disconnect! ${client!!.isDisposed}, ${client!!.channel().isOpen}, ${client!!.channel().isActive}, ${client!!.channel().remoteAddress()}"
            )
        }
            .doOnNext { text ->
                println(text)
                if (text == "ok") {
                    println(" [x] Done! Remove $message from queue!")
                    channel.basicAck(delivery.envelope.deliveryTag, false)
                }
            }
            .doOnError { err -> println(err.message); }
            .subscribe()
    }


    fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }
}
