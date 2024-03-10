package ru.foxstudios.marsbridge.service

import com.fasterxml.jackson.databind.ObjectMapper
import com.rabbitmq.client.Channel
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.DeliverCallback
import com.rabbitmq.client.Delivery
import io.netty.channel.ChannelOption
import kotlinx.coroutines.runBlocking
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.udp.UdpClient
import ru.foxstudios.marsbridge.model.ScheduleModel
import java.io.BufferedReader
import java.io.InputStreamReader
import java.net.HttpURLConnection
import java.net.URL
import java.nio.charset.StandardCharsets
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter
import kotlin.math.min


class EarthTransferService {
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
        channel.addShutdownListener { text -> error(text) }
        //client!!.onDispose().block()
    }

    fun doWork(message: String, channel: Channel, delivery: Delivery) {
        println("test-earth-ip ${System.getenv("EARTH_IP")} port:${System.getenv("PORT")}")
        val scheduleString = getSchedule()
        val mapper = ObjectMapper()
        val schedule = mapper.readValue(scheduleString, ScheduleModel::class.java)
        val weight = runBlocking {
            countMessageWeight(message)
        }
        if (validateSchedule(schedule,weight)) {
            client =
                UdpClient.create().host(System.getenv("EARTH_IP")).port(System.getenv("PORT").toInt()).wiretap(true)
                    .option(
                        ChannelOption.SO_SNDBUF,
                        8192
                    ).connectNow()
            println(" [d] isDisposed true ${client!!.isDisposed}")


            println(" [x] Received '$message' weight: $weight")

            //client!!.outbound().sendString(Mono.just(message)).then().subscribe()

            val size = message.toByteArray()
            var testString = ""
            val weightLocal = 256
            if (size.size > weightLocal) {
                println("here, current bufferedSize: ${size.size}")
                val list = ArrayList<ByteArray>()
                var i = 0
                var j = 0
                while (i < size.size) {
                    list.add(size.slice(i..(min(size.size, i + weightLocal) - 1)).toByteArray())
                    println("logging: $j current listSize: ${list.size}")
                    j += 1
                    i += weightLocal
                }
                println("exit")
                for (elem in list) {
                    testString += elem.toString(StandardCharsets.UTF_8)
                    println("part sended!, part: ${elem.toString(StandardCharsets.UTF_8)}")
                    client!!.outbound().sendByteArray(Mono.just(elem)).then().subscribe()
                }


            } else {
                println("sendLittleMessage!")
                client!!.outbound().sendByteArray(Mono.just(message.toByteArray())).then().subscribe()
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
                    } else {
                        println(" [*] Reveived: $text")
                    }
                }
                .doOnError { err -> println(err.message); }
                .subscribe()
        }
    }


    fun countMessageWeight(message: String): Int {
        return message.toByteArray(Charsets.UTF_8).size
    }

    fun getSchedule(): String {
        val url = URL("http://localhost:30007/data/getschedule")
        val con = url.openConnection() as HttpURLConnection
        con.requestMethod = "GET"
        con.setRequestProperty("Content-type", "application/json")
        val status = con.responseCode
        val content = StringBuffer()
        if (status == 200) {
            val `in` = BufferedReader(
                InputStreamReader(con.inputStream)
            )
            var inputLine: String?

            while (`in`.readLine().also { inputLine = it } != null) {
                content.append(inputLine)
            }
            `in`.close()

        }
        return content.toString()
    }

    fun validateSchedule(scheduleModel: ScheduleModel,modelWeight:Int): Boolean {

        val startTime = LocalDateTime.now()
        val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
        val scheduleStartTime = LocalDateTime.parse(scheduleModel.fromD,formatter)
        val scheduleTimeEnd = LocalDateTime.parse(scheduleModel.toD, formatter)

        val time = modelWeight.toDouble()/scheduleModel.speed.toDouble()
        val timeRelease = startTime.plusSeconds(time.toLong())

        println(time)
        println(timeRelease)
        println(scheduleTimeEnd)
        return scheduleTimeEnd > timeRelease
    }
}
