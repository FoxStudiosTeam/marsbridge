package ru.foxstudios.marsbridge

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import reactor.netty.Connection
import reactor.netty.udp.UdpClient
import reactor.netty.udp.UdpServer
import ru.foxstudios.marsbridge.service.EarthTransferService
import java.time.Duration

fun main(args: Array<String>) {
    val client: Connection = UdpClient.create().port(25577).host("host.docker.internal").connectNow()
    val rmqService = EarthTransferService(client)
    runBlocking {
        launch {

            client.inbound().receive().asString().doOnNext{text ->
                println(text)
            }.doOnError{err-> println(err.message); client.disposeNow()}.subscribe()
        }
        println("starting1")
    }
    client.onDispose().block()
}
