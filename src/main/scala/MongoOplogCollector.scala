import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior}
import akka.util.Timeout
import com.mongodb.CursorType
import org.bson.BsonObjectId
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp, ObjectId}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.{MongoDatabase, Observable, Observer}

import scala.concurrent.duration._


class MongoOplogCollector(
  oplogDB: MongoDatabase,
  sourceDB: MongoDatabase,
  ops: Seq[String],
  timers: TimerScheduler[MongoOplogCollector.Command],
  cursorActor: ActorRef[CursorManager.Command],
  context: ActorContext[MongoOplogCollector.Command]
)
  extends AbstractBehavior[MongoOplogCollector.Command](context) {

  import MongoOplogCollector._

  private val coll = oplogDB.getCollection("oplog.rs")

  private var currentCursor: CursorManager.Cursor = _

  override def onMessage(
    msg: MongoOplogCollector.Command
  ): Behavior[MongoOplogCollector.Command] = {
    val cursorAdapter: ActorRef[CursorManager.Response] =
      context.messageAdapter(msg => WrappedCursorResponse(msg))
    val dumperAdapter: ActorRef[OplogDumper.Response] =
      context.messageAdapter(msg => WrappedDumperResponse(msg))
    val receptionistAdapter: ActorRef[Receptionist.Listing] =
      context.messageAdapter(msg => WrappedReceptionistResponse(msg))

    implicit val timeout: Timeout = 3.seconds

    msg match {
      case _: Start                             =>
        cursorActor ! CursorManager.Get(cursorAdapter)
      case _: Stop                              =>
      case _: Suspend                           =>
      case _: Resume                            =>
      case _: UpdateCursor                      =>
        cursorActor ! CursorManager.Update(currentCursor.ts, currentCursor.inc)
      case Query(db, coll, id)                  =>
        val dumperKey = s"${db}.${coll}:${id}"
        val serviceKey = OplogDumper.DumperServiceKey(dumperKey)
        context.system.receptionist ! Receptionist.Find(serviceKey, receptionistAdapter)
      case WrappedReceptionistResponse(listing) =>
        val key = OplogDumper.DumperServiceKey(listing.key.id)
        listing.allServiceInstances(key).headOption.foreach(dumper =>
          dumper ! OplogDumper.Get(dumperAdapter)
        )
      case WrappedDumperResponse(data)          =>
        context.log.info("query data: {}", data)

      case _@WrappedCursorResponse(resp)        =>
        val cursor = resp.asInstanceOf[CursorManager.Cursor]

        timers.startTimerAtFixedRate(UpdateCursor(), 1.second)

        val oplogs = init(BsonTimestamp(cursor.ts.toInt, cursor.inc), ops)

        oplogs.subscribe(new Observer[Oplog] {
          override def onNext(oplog: Oplog): Unit = {
            currentCursor = CursorManager.Cursor(oplog.ts.getTime, oplog.ts.getInc)

            // a dumper is a long lived actor, it should watch by this context
            // and discovered by this context to get its state.
            val dumper = context.spawn(OplogDumper(sourceDB), idOfDumper(oplog))

            // watch this dumper
            context.watch(dumper)

            // register this long live dumper
            // why not use children? TODO
            context.system.receptionist ! Receptionist.Register(OplogDumper.DumperServiceKey(idOfDumper(oplog)), dumper)

            dumper ! OplogDumper.Set(oplog)

            context.self ! Query("sdrc", "test", oplog.id.toHexString)
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

  private def nsFilter(doc: Document) = {
    val ns = doc.get("ns").get.asInstanceOf[BsonString].getValue
    ns.startsWith("sdrc") // TODO fix this
  }
}

object MongoOplogCollector {

  def apply(oplogDB: MongoDatabase, sourceDB: MongoDatabase, ops: Seq[String], cursorActor: ActorRef[CursorManager.Command]): Behavior[Command] =
    Behaviors.withTimers[Command](timers => {
      Behaviors.setup[Command](context => new MongoOplogCollector(oplogDB, sourceDB, ops, timers, cursorActor, context))
    })


  sealed trait Command

  case class Start() extends Command

  case class Stop() extends Command

  case class Suspend() extends Command

  case class Resume() extends Command

  case class Query(db: String, coll: String, id: String) extends Command

  private case class UpdateCursor() extends Command

  private case class WrappedCursorResponse(resp: CursorManager.Response) extends Command

  private case class WrappedDumperResponse(resp: OplogDumper.Response) extends Command

  private case class WrappedReceptionistResponse(list: Receptionist.Listing) extends Command

}
