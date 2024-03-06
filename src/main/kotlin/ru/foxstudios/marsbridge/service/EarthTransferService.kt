package ru.foxstudios.marsbridge.service

import com.rabbitmq.client.ConnectionFactory
import java.lang.Exception

class EarthTransferService() {
    init {
        val factory = ConnectionFactory()
        factory.host = "localhost"
        factory.port = 5672
        try {
            //test
        } catch (e: Exception) {

        }
    }
}
