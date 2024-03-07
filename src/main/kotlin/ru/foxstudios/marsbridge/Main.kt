package ru.foxstudios.marsbridge

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.udp.UdpClient

fun main(args: Array<String>) {
    val client: Connection =
        UdpClient.create().port(25577).host("host.docker.internal").wiretap(true).connectNow()

    val factory = ConnectionFactory()
    factory.host = "mars-queue-service"
    factory.port = 5672
    val connection: com.rabbitmq.client.Connection = factory.newConnection()
    val channel = connection.createChannel()

    val deliverCallback: DeliverCallback = DeliverCallback { _, delivery ->
        val message = String(delivery.body, charset("UTF-8"))
        try{
            doWork(client,message)
        }finally {
            println(" [x] Done - ok?!")
        }

    }

    channel.basicConsume("mars-queue", true, deliverCallback, { consumerTag -> })
    println("starting1")

    client.onDispose().block()
}

fun doWork(client: Connection, message: String) {
    val weight = runBlocking {
        countMessageWeight(message)
    }
    println(" [x] Received '$message' weight: $weight")
    client.outbound().sendString(Mono.just(message)).then().subscribe()

    client.inbound().receive().asString().doOnTerminate {
        println(
            "disconnect! ${client.isDisposed}, ${client.channel().isOpen}, ${client.channel().isActive}, ${
                client.channel().remoteAddress()
            }"
        )
    }
        .doOnNext { text ->
            println(text)
            if (text == "ok") {
                //channel.basicAck(delivery.envelope.deliveryTag, false)
                println(" [x] Done! Remove $message from queue!")
            }
        }
        .doOnError { err -> println(err.message); }
        .subscribe()
}


fun countMessageWeight(message: String): Int {
    return message.toByteArray(Charsets.UTF_8).size
}
