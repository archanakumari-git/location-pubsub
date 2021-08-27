package com.lightbend.location
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.collection.JavaConverters._

object LocationConsumer {

  def main(args: Array[String]): Unit = {
    consumeFromKafka("location-request")
  }

  def consumeFromKafka(topic: String) = {
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")
    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    val logger = LoggerFactory.getLogger("location-consumer")
    while (true) {
      logger.info("consumer-side")
      val record = consumer.poll(1000).asScala
      for (data <- record.iterator) {
        logger.info(data.value())
        println(data.value())
      }
    }
  }
}