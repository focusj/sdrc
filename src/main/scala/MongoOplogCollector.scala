import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.mongodb.CursorType
import org.bson.BsonObjectId
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp, ObjectId}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.{MongoDatabase, Observable, Observer}

import scala.concurrent.duration._


class MongoOplogCollector(
  db: MongoDatabase,
  ops: Seq[String],
  timers: TimerScheduler[MongoOplogCollector.Command],
  cursorActor: ActorRef[CursorManager.Command],
  context: ActorContext[MongoOplogCollector.Command]
)
  extends AbstractBehavior[MongoOplogCollector.Command](context) {

  import MongoOplogCollector._

  private val coll = db.getCollection("oplog.rs")

  private var currentCursor: CursorManager.Cursor = _

  override def onMessage(
    msg: MongoOplogCollector.Command
  ): Behavior[MongoOplogCollector.Command] = {
    val cursorAdapter: ActorRef[CursorManager.Response] =
      context.messageAdapter(msg => WrappedCursorResponse(msg))
    val dumperAdapter: ActorRef[OplogDumper.Response] =
      context.messageAdapter(msg => WrappedDumperResponse(msg))

    msg match {
      case _: Start                      =>
        cursorActor ! CursorManager.Get(cursorAdapter)
      case _: Stop                       =>
      case _: Suspend                    =>
      case _: Resume                     =>
      case _: UpdateCursor               =>
        cursorActor ! CursorManager.Update(currentCursor.ts, currentCursor.inc)
      case _@WrappedCursorResponse(resp) =>
        val cursor = resp.asInstanceOf[CursorManager.Cursor]

        timers.startTimerAtFixedRate(UpdateCursor(), 1.second)

        val oplogs = init(BsonTimestamp(cursor.ts.toInt, cursor.inc), ops)

        oplogs.subscribe(new Observer[Oplog] {
          override def onNext(oplog: Oplog): Unit = {
            currentCursor = CursorManager.Cursor(oplog.ts.getTime, oplog.ts.getInc)
            val dumper = context.spawn(OplogDumper(db), idOfDumper(oplog))
            context.watch(dumper)
            dumper ! OplogDumper.Set(oplog)
          }

          override def onError(e: Throwable): Unit = {
            context.log.error("{}", e)
            // TODO error handling
          }

          override def onComplete(): Unit = {
            context.log.info("consuming oplog done")
            // TODO how to finish
          }
        })
    }

    this
  }

  def idOfDumper(oplog: Oplog) = {
    oplog.ns + ":" + oplog.id.toHexString
  }

  def init(from: BsonTimestamp, ops: Seq[String]): Observable[Oplog] = {
    val query = and(
      Filters.gt("ts", from),
      Filters.in("op", ops: _*),
      Filters.exists("fromMigrate", exists = false)
    )
    coll
      .find(query)
      .oplogReplay(true)
      .cursorType(CursorType.Tailable)
      .noCursorTimeout(true)
      .withFilter(doc => nsFilter(doc))
      .map(trans)
      .observeOn(scala.concurrent.ExecutionContext.global)
  }

  private def trans(doc: Document) = {
    val op = doc.get("op").get.asInstanceOf[BsonString]
    val ns = doc.get("ns").get.asInstanceOf[BsonString]
    val ts = doc.get("ts").get.asInstanceOf[BsonTimestamp]
    val obj = doc.get("o").get.asInstanceOf[BsonDocument]
    Oplog(idOfOplog(doc), op.getValue, ns.getValue, ts, obj)
  }

  private def nsFilter(doc: Document) = {
    val ns = doc.get("ns").get.asInstanceOf[BsonString].getValue
    ns.startsWith("sdrc") // TODO fix this
  }

  private def idOfOplog(doc: Document): ObjectId = {
    context.log.info("{}", doc)
    val o = doc.get("op").get.asInstanceOf[BsonString]
    o.getValue match {
      case "u"       =>
        doc.get("o2").get.asInstanceOf[BsonObjectId].getValue
      case "d" | "i" =>
        doc.get("o").get.asInstanceOf[BsonDocument].get("_id").asInstanceOf[BsonObjectId].getValue
      case op        =>
        throw new IllegalArgumentException(s"not support op: {$op} exception")
    }
  }
}

object MongoOplogCollector {

  def apply(db: MongoDatabase, ops: Seq[String], cursorActor: ActorRef[CursorManager.Command]): Behavior[Command] =
    Behaviors.withTimers[Command](timers => {
      Behaviors.setup[Command](context => new MongoOplogCollector(db, ops, timers, cursorActor, context))
    })


  sealed trait Command

  case class Start() extends Command

  case class Stop() extends Command

  case class Suspend() extends Command

  case class Resume() extends Command

  case class UpdateCursor() extends Command

  case class WrappedCursorResponse(resp: CursorManager.Response) extends Command

  case class WrappedDumperResponse(resp: OplogDumper.Response) extends Command

}
