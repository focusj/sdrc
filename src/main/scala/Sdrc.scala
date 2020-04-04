import java.util.concurrent.CountDownLatch

import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.Observer
import org.mongodb.scala.bson.BsonTimestamp

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._


trait Configurable {
  def config(): Config = ConfigFactory.load("sdrc")
}

case class OplogConf(after: BsonTimestamp, ops: List[String])

case class Oplog(op: String /*, ns: String, ts: Long, doc: Map[String, Object]*/)

object Sdrc extends App with Configurable {

  val cursorManager: CursorManager = new {
    val uri: String = config().getString("sdrc.cursor.mongo.uri")
    val database: String = config().getString("sdrc.cursor.mongo.database")
  } with CursorManager with Mongoable with Configurable

  val kafkaBroker: KafkaBroker = new {
    override val topic: String = ""
    override val kafkaHost: String = ""
  } with KafkaBroker

  val mongoOplogCollector: MongoOplogCollector = new {
    val uri: String = config().getString("sdrc.collector.mongo.uri")
    val database: String = config().getString("sdrc.collector.mongo.database")
    val cursorManager = this.cursorManager
    val kafkaBroker = this.kafkaBroker
  } with MongoOplogCollector with Mongoable with Configurable

  cursorManager.get.map(cursor => {
    val ops = config().getStringList("sdrc.collector.mongo.ops").asScala
    val oplogs = mongoOplogCollector.run(cursor, ops)
    oplogs.subscribe(new Observer[Oplog] {
      override def onNext(oplog: Oplog): Unit =
      //kafkaBroker.push(oplog)
        println(oplog)

      override def onError(e: Throwable): Unit =
        println(e)

      override def onComplete(): Unit =
        print("done")
    })
  })
  val latch = new CountDownLatch(1)
  latch.await()
}
