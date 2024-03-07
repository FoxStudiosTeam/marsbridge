package ru.foxstudios.marsbridge

import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.udp.UdpClient

fun main(args: Array<String>) {
    val client: Connection =
        UdpClient.create().port(25577).host("host.docker.internal").wiretap(true).doOnDisconnected {
            it.rebind(reconnect())
            println("DISCONNECTED!!!!!!!!!!")
        }.connectNow()

    val factory = ConnectionFactory()
    factory.host = "mars-queue-service"
    factory.port = 5672
    try {

        val connection: com.rabbitmq.client.Connection = factory.newConnection()
        val channel = connection.createChannel()
        val deliverCallback: DeliverCallback = DeliverCallback { _, delivery ->
            val message = String(delivery.body, charset("UTF-8"))
            val weight = runBlocking {
                countMessageWeight(message)
            }

            println(" [x] Received '$message' weight: $weight")
            client.outbound().sendString(Mono.just(message)).then().subscribe()
            client.inbound().receive().asString().doOnTerminate {
                println("disconnect! ${client.isDisposed}")
            }
                .doOnNext { text ->
                    println(text)
                    if (text == "ok") {
                        channel.basicAck(delivery.envelope.deliveryTag, false)
                        println(" [x] Done! Remove $message from queue!")
                    }
                }
                .doOnError { err -> println(err.message); client.disposeNow() }
                .subscribe()
        }

        channel.basicConsume("mars-queue", false, deliverCallback, { consumerTag -> })
    } catch (e: Exception) {
        println(e.message)
        e.printStackTrace()
    }



    println("starting1")

    client.onDispose().block()
}

fun reconnect(): Connection {
    Thread.sleep(100000)
    print("reconecting......")
    return UdpClient.create().port(25577).host("host.docker.internal").doOnDisconnected {
        reconnect()
    }.connectNow()
}

private suspend fun countMessageWeight(message: String): Int {
    return message.toByteArray(Charsets.UTF_8).size
}
