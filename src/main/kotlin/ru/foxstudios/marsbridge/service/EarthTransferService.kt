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
import kotlin.math.min


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
        channel.addShutdownListener{text-> error(text) }
        //client!!.onDispose().block()
    }

    fun doWork(message: String, channel: Channel, delivery: Delivery) {

        client = UdpClient.create().port(25577).host("host.docker.internal").wiretap(true).connectNow()
        println(" [d] isDisposed true ${client!!.isDisposed}")

        val weight = runBlocking {
            countMessageWeight(message)
        }
        println(" [x] Received '$message' weight: $weight")
        val file = File("tmp/file.json")
        FileUtils.touch(file)
        FileUtils.writeByteArrayToFile(file, message.toByteArray())
        println(file.readText())
        //client!!.outbound().sendString(Mono.just(message)).then().subscribe()

        val size = file.readBytes()
        if (size.size > 40972) {
            val list = ArrayList<ByteArray>()
            var i = 0
            while (i < size.size) {
                list.add(size.slice(i..min(size.size, i + 40972)).toByteArray())
                i += 40972
            }

            for (elem in list) {
                client!!.outbound().sendByteArray(Mono.just(elem)).then().subscribe()
            }



        } else {
            client!!.outbound().sendByteArray(Mono.just(file.readBytes())).then().subscribe()
        }



        client!!.inbound().receive().asString().doOnTerminate {
            println(
                "disconnect! ${client!!.isDisposed}, ${client!!.channel().isOpen}, ${client!!.channel().isActive}, ${
                    client!!.channel().remoteAddress()
                }"
            )
        }
            .doOnNext { text ->
                if (text == "ok") {
                    println(" [x] Done! Remove $message from queue!")
                    channel.basicAck(delivery.envelope.deliveryTag, false)
                }else{
                    println(" [*] Reveived: $text")
                }
            }
            .doOnError { err -> println(err.message); }
            .subscribe()
    }


    fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }
}
