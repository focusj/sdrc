import java.util.concurrent.CountDownLatch

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.bson.{BsonDocument, BsonTimestamp}
import org.mongodb.scala.{MongoClient, MongoDatabase}

trait Configurable {
  def config(): Config = ConfigFactory.load("sdrc")
}

case class OplogConf(after: BsonTimestamp, ops: List[String])

case class Oplog(op: String, ns: String, ts: Long, doc: BsonDocument)

object Sdrc extends App with Configurable {
  lazy val client: MongoClient = MongoClient(uri)
  lazy val db: MongoDatabase = client.getDatabase(database)
  val uri: String = config().getString("sdrc.cursor.mongo.uri")
  val database: String = config().getString("sdrc.cursor.mongo.database")
  val latch = new CountDownLatch(1)
  private val sdrc: ActorSystem[Nothing] = ActorSystem(Behaviors.setup[Any](context => {
    val cursorActor = context.spawn(CursorManager(db), "cursor-actor")
    val oplogActor = context.spawn(MongoOplogCollector(db, cursorActor), "oplog-actor")

    oplogActor ! MongoOplogCollector.Start()

    Behaviors.same
  }), "sdrc")
  latch.await()
}
