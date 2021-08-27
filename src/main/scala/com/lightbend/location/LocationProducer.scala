package com.lightbend.location

import java.util.Properties
import org.apache.kafka.clients.producer._
import org.slf4j.LoggerFactory
import net.liftweb.json._
import com.lightbend.location.entity.{LocationRequest, LocationResponse}
import net.liftweb.json.Serialization.write

object LocationProducer {

  def main(args: Array[String]): Unit = {
    writeToKafka("location-response")
  }

  def randomLocationResponse(locationJsonString: String): String = {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val locationRequest = parse(locationJsonString).extract[LocationRequest]
    val location = if (locationRequest.zipCode > 99999 && locationRequest.zipCode < 1000000) {
      LocationResponse(locationRequest.zipCode, false, locationRequest.userId, locationRequest.requestId)
    }
    else {
      LocationResponse(locationRequest.zipCode, true, locationRequest.userId, locationRequest.requestId)
    }
    val jsonString = write(location)
    println(jsonString)
    jsonString
  }

  def writeToKafka(topic: String): Unit = {
    val logger = LoggerFactory.getLogger("location-producer")
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)
    //todo : change it when integrating with orch
    val zipcodes = Seq("123456", "678956", "560038", "234")
    val locationsToPublish = zipcodes.map(zipcode => randomLocationResponse(zipcode))
    locationsToPublish.map {
      loc =>
        val record = new ProducerRecord[String, String](topic,
          loc)
        logger.info(record.topic())
        logger.info(record.key())
        logger.info(record.value())
        producer.send(record)
    }
    producer.close()
  }
}
