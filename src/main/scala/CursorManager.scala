import java.time.Instant

import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.model.FindOneAndReplaceOptions

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future
import scala.util.{Failure, Success}


class CursorManager(
  db: MongoDatabase,
  context: ActorContext[CursorManager.Command]
) extends AbstractBehavior[CursorManager.Command](context) {

  import CursorManager._

  private val coll = db.getCollection("cursor")

  override def onMessage(
    msg: CursorManager.Command
  ): Behavior[CursorManager.Command] = {
    msg match {
      case _@Get(reply)                =>
        context.pipeToSelf(get) {
          case Success(ts) => GetSuccess(Cursor(ts.getValue, ts.getInc), reply)
          case Failure(ex) => GetFailed(ex, reply)
        }
      case _@GetSuccess(cursor, reply) =>
        reply ! cursor
      case _@GetFailed(ex, reply)      =>
        context.log.warn("get cursor failed: {}", ex)
        val ts = defaultTimestamp
        reply ! Cursor(ts.getValue, ts.getInc)
      case _@Update(ts, inc)           =>
        update(ts, inc)
    }
    this
  }

  private def get: Future[BsonTimestamp] = {
    coll.find(Document("_id" -> 1)).headOption().map {
      case Some(v) =>
        BsonTimestamp(v.getInteger("time"), v.getInteger("inc"))
      case None    =>
        defaultTimestamp
    }
  }

  private def defaultTimestamp = {
    BsonTimestamp(0, 0)
  }

  private def update(time: Long, inc: Int): Unit = {
    val opt = FindOneAndReplaceOptions().upsert(true)
    coll.findOneAndReplace(
      Document("_id" -> 1),
      Document("time" -> time, "inc" -> inc),
      opt
    )
  }
}

object CursorManager {
  def apply(db: MongoDatabase): Behavior[Command] =
    Behaviors.setup[Command](context => new CursorManager(db, context))

  sealed trait Command

  private case class GetSuccess(cursor: Cursor, reply: ActorRef[Response]) extends Command

  private case class GetFailed(ex: Throwable, reply: ActorRef[Response]) extends Command

  case class Get(reply: ActorRef[Response]) extends Command

  case class Update(ts: Long, inc: Int) extends Command

  sealed trait Response

  case class Cursor(ts: Long, inc: Int) extends Response

}
