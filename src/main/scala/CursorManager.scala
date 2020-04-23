import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.model.FindOneAndReplaceOptions
import org.mongodb.scala.{MongoDatabase, Observer}

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
      case Get(reply)                =>
        context.pipeToSelf(get) {
          case Success(ts) => GetSuccess(Cursor(ts.getTime, ts.getInc), reply)
          case Failure(ex) => GetFailed(ex, reply)
        }
      case GetSuccess(cursor, reply) =>
        reply ! cursor
      case GetFailed(ex, reply)      =>
        context.log.warn("get cursor failed: {}", ex)
        val ts = defaultTimestamp
        reply ! Cursor(ts.getValue, ts.getInc)
      case Update(ts, inc)           =>
        context.log.info("update cursor {}, {}", ts, inc)
        update(ts, inc)
    }
    this
  }

  private def get: Future[BsonTimestamp] = {
    coll.find(Document("_id" -> 1)).headOption().map {
      case Some(doc) =>
        val ts = doc.getOrElse("time", 0).asNumber().intValue()
        val inc = doc.getOrElse("inc", 0).asNumber().intValue()
        BsonTimestamp(ts, inc)
      case None      =>
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
    ).subscribe(new Observer[Document] {
      override def onNext(result: Document): Unit = context.log.info("update result: {}", result)

      override def onError(e: Throwable): Unit = context.log.error("update failed: {}", e)

      override def onComplete(): Unit = context.log.info("update done")
    })
  }
}

object CursorManager {
  def apply(db: MongoDatabase): Behavior[Command] =
    Behaviors.setup[Command](context => new CursorManager(db, context))

  sealed trait Command

  sealed trait Response

  case class Get(reply: ActorRef[Response]) extends Command

  case class Update(ts: Long, inc: Int) extends Command

  case class Cursor(ts: Long, inc: Int) extends Response

  private case class GetSuccess(cursor: Cursor, reply: ActorRef[Response]) extends Command

  private case class GetFailed(ex: Throwable, reply: ActorRef[Response]) extends Command

}
