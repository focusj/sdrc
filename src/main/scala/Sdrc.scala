import java.util.concurrent.CountDownLatch

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.bson.{BsonDocument, BsonTimestamp, ObjectId}
import org.mongodb.scala.{MongoClient, MongoDatabase}

import scala.collection.JavaConverters._

trait Configurable {
  def config(): Config = ConfigFactory.load("sdrc")
}

case class OplogConf(after: BsonTimestamp, ops: List[String])

case class Oplog(id: ObjectId, op: String, ns: String, ts: BsonTimestamp, doc: BsonDocument) {
  def key(): String = {
    ns + ":" + id.toHexString
  }
}

object Sdrc extends App with Configurable {

  private val sdrc: ActorSystem[Nothing] = ActorSystem(Behaviors.setup[Any](context => {
    val cursorDb = getDb(config().getString("sdrc.cursor.mongo.uri"),
      config().getString("sdrc.cursor.mongo.database"))

    val oplogDb = getDb(config().getString("sdrc.collector.mongo.uri"),
      config().getString("sdrc.collector.mongo.database"))

    val sourceDb = getDb(config().getString("sdrc.collector.mongo.uri"), "sdrc")

    val cursorActor = context.spawn(CursorManager(cursorDb), "cursor-actor")

    val ops = config().getStringList("sdrc.collector.mongo.ops").asScala.toSet
    val oplogActor = context.spawn(Collector(oplogDb, sourceDb, ops, cursorActor), "oplog-actor")

    oplogActor ! Collector.Start()

    Behaviors.same
  }), "sdrc")

  new CountDownLatch(1).await()

  private def getDb(address: String, dbName: String): MongoDatabase = {
    MongoClient(address).getDatabase(dbName)
  }

}
