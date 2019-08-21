import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch

import com.mongodb.CursorType
import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{MongoClient, MongoDatabase, Observer}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait MongoComponent {
  def db(): MongoDatabase
}

trait CursorMongo extends MongoComponent with Configurable {
  def db(): MongoDatabase = {
    val client = MongoClient("mongodb://127.0.0.1")
    client.getDatabase(config().getString("collector.cursor.mongo"))
  }
}

trait OplogMongo extends MongoComponent with Configurable {
  def db(): MongoDatabase = {
    val client = MongoClient("mongodb://127.0.0.1")
    client.getDatabase(config().getString("collector.oplog.mongo"))
  }
}

trait CursorManager {
  this: MongoComponent =>

  private val coll = db().getCollection("cursor")

  def getCursor: Future[BsonTimestamp] = {
    coll.find().headOption().map {
      case Some(v) =>
        BsonTimestamp(v.getInteger("time"), v.getInteger("inc"))
      case None    =>
        BsonTimestamp(Instant.now().getEpochSecond.toInt, 0)
    }
  }

  def updateCursor(time: Int, inc: Int): Unit = {
    coll.updateOne(Document(), Document("time" -> time, "inc" -> inc))
  }
}

trait Configurable {
  def config(): Config = ConfigFactory.load("sdrc")
}

case class OplogConf(after: BsonTimestamp, ops: List[String])

case class Oplog(op: String)

class MongoCollector {
  this: MongoComponent with CursorManager with Configurable =>

  private val coll = db().getCollection("oplog.rs")

  private def trans(doc: Document) = {
    print(doc)
    Oplog("op")
  }


  def run() = {
    getCursor.map(cursor => {
      val ops = config().getStringList("collector.ops").asScala
      val query = and(
        gt("ts", cursor),
        in("op", ops: _*),

        exists("fromMigrate", exists = false)
      )
      coll.find(query)
          .oplogReplay(true)
          .cursorType(CursorType.Tailable)
          .noCursorTimeout(true)
          .map(trans)
    })

  }
}

object Sdrc extends App {
  val mongo: MongoClient = MongoClient("mongodb://127.0.0.1")

  val cursorManager = new {} with CursorManager with CursorMongo
  val mongoCollector = new {} with MongoCollector with OplogMongo with CursorManager with Configurable


  val oplogObs = mongoCollector.run()

  oplogObs.onComplete({
    case Success(oplogs)    =>
      oplogs.subscribe(new Observer[Oplog] {
        override def onNext(result: Oplog): Unit = println(result)

        override def onError(e: Throwable): Unit = println(e)

        override def onComplete(): Unit = print("done")
      })
    case Failure(exception) =>
      print(exception)
  })


  val latch = new CountDownLatch(1)
  latch.await()
}
