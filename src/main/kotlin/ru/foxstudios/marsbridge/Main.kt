package ru.foxstudios.marsbridge

import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import reactor.netty.Connection
import reactor.netty.udp.UdpClient
import ru.foxstudios.marsbridge.service.EarthTransferService

fun main(args: Array<String>) {
    val client: Connection = UdpClient.create().port(25577).host("host.docker.internal").connectNow()
    val rmqService = EarthTransferService(client)
    runBlocking {
        launch {
            client.inbound().receive().asString().doOnTerminate {


            }
                .doOnNext { text -> println(text) }
                .doOnError { err -> println(err.message); client.disposeNow() }
                .subscribe()


        }
        println("starting1")
    }
    client.onDispose().block()
}

fun reconnect(): Connection {
    Thread.sleep(100000)
    print("reconecting......")
    return UdpClient.create().port(25577).host("host.docker.internal").doOnDisconnected {
        reconnect()
    }.connectNow()
}
