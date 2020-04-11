import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import com.mongodb.CursorType
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp}
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
    val adapter: ActorRef[CursorManager.Response] = context.messageAdapter(msg => WrappedResponse(msg))

    msg match {
      case _: Start                =>
        cursorActor ! CursorManager.Get(adapter)
      case _: Stop                 =>
      case _: Suspend              =>
      case _: Resume               =>
      case _: UpdateCursor         =>
        cursorActor ! CursorManager.Update(currentCursor.ts, currentCursor.inc)
      case _@WrappedResponse(resp) =>
        val cursor = resp.asInstanceOf[CursorManager.Cursor]

        timers.startTimerAtFixedRate(UpdateCursor(), 1.second)

        val oplogs = init(BsonTimestamp(cursor.ts.toInt, cursor.inc), ops)

        oplogs.subscribe(new Observer[Oplog] {
          override def onNext(oplog: Oplog): Unit = {
            currentCursor = CursorManager.Cursor(oplog.ts.getTime, oplog.ts.getInc)
            context.log.info("{}", oplog)
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
      .map(trans)
      .observeOn(scala.concurrent.ExecutionContext.global)
  }

  //  Document(
  //    (ts,Timestamp{value=6811811109184471247, seconds=1585998365, inc=207}),
  //    (t,BsonInt64{value=141}),
  //    (h,BsonInt64{value=2129071645297751756}),
  //    (v,BsonInt32{value=2}),
  //    (op,BsonString{value='u'}),
  //    (ns,BsonString{value='crowdsourcing.tiktok_task'}),
  //    (o2,{"_id": {"$numberLong": "6809163828178305548"}}),
  //    (o,{"$set": {"modify_time": {"$date": 1586027165378}}})
  //  )
  // TODO oplog parse
  private def trans(doc: Document) = {
    val op = doc.get("op").get.asInstanceOf[BsonString]
    val ns = doc.get("ns").get.asInstanceOf[BsonString]
    val ts = doc.get("ts").get.asInstanceOf[BsonTimestamp]
    val obj = doc.get("o").get.asInstanceOf[BsonDocument]
    val id = null //doc.get("o2").map(_.asInstanceOf[ObjectId]).orNull
    Oplog(id, op.getValue, ns.getValue, ts, obj)
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

  case class WrappedResponse(resp: CursorManager.Response) extends Command

}
