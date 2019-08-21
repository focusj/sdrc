import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch

import com.mongodb.CursorType
import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.{MongoClient, MongoDatabase, Observable, Observer}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}

trait MongoComponent {
  val uri: String
  val database: String

  def client(): MongoClient = MongoClient(uri)

  def db(): MongoDatabase = client().getDatabase(database)
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

trait MongoOplogCollector {
  this: MongoComponent with Configurable =>

  val cursorManager: CursorManager

  private val coll = db().getCollection("oplog.rs")

  private def trans(doc: Document) = {
    print(doc)
    Oplog("op")
  }


  def run(): Future[Observable[Oplog]] = {
    cursorManager.getCursor.map(cursor => {
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

object Sdrc extends App with Configurable {

  val cursorManager: CursorManager = new {
    val uri: String = "mongodb://127.0.0.1"
    val database: String = config().getString("collector.cursor.mongo")
  } with CursorManager with MongoComponent with Configurable

  val mongoOplogCollector: MongoOplogCollector = new {
    val uri: String = "mongodb://127.0.0.1"
    val database: String = config().getString("collector.oplog.mongo")
    val cursorManager = this.cursorManager
  } with MongoOplogCollector with MongoComponent with Configurable

  val oplogObs = mongoOplogCollector.run()

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
