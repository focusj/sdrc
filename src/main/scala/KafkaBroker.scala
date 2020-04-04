import java.util.Properties

import Sdrc.config
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

trait KafkaBroker {
  val topic: String
  val kafkaHost: String

  val props = new Properties()
  props.put("bootstrap.servers", config().getString("sdrc.broker.host"))
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val producer: KafkaProducer[String, Oplog] = new KafkaProducer[String, Oplog](props)

  def push(oplog: Oplog): Unit = {
    val record = new ProducerRecord[String, Oplog]("key", oplog)
    producer.send(record)
  }
}
