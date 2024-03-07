package ru.foxstudios.marsbridge

import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import reactor.netty.Connection
import reactor.netty.udp.UdpClient
import reactor.netty.udp.UdpServer
import ru.foxstudios.marsbridge.service.EarthTransferService
import java.time.Duration

fun main(args: Array<String>) {
    val client: Connection = UdpClient.create().port(25577).host("127.0.0.1").connectNow()
    val rmqService = EarthTransferService(client)
    runBlocking {
        launch {

            println("starting2")
        }
        println("starting1")
    }
    client.onDispose().block()
}
