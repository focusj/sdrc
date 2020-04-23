import Implicits.MongoDocumentImplicits
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.util.Timeout
import com.mongodb.CursorType
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.{MongoDatabase, Observable, Observer}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}


class Collector(
  oplogDB: MongoDatabase,
  sourceDB: MongoDatabase,
  ops: Set[String],
  timers: TimerScheduler[Collector.Command],
  cursorActor: ActorRef[CursorManager.Command],
  context: ActorContext[Collector.Command]
)
  extends AbstractBehavior[Collector.Command](context) {

  import Collector._
  import akka.actor.typed.scaladsl.AskPattern._

  private val coll = oplogDB.getCollection("oplog.rs")
  private val dumperRefs = mutable.HashMap.empty[String, ActorRef[Dumper.Command]]
  private var currentCursor: CursorManager.Cursor = _

  override def onMessage(
    msg: Collector.Command
  ): Behavior[Collector.Command] = {
    val cursorAdapter: ActorRef[CursorManager.Response] =
      context.messageAdapter(msg => WrappedCursorResponse(msg))
    val dumperAdapter: ActorRef[Dumper.Response] =
      context.messageAdapter(msg => WrappedDumperResponse(msg))

    implicit val timeout: Timeout = 500.millis
    implicit val scheduler: Scheduler = context.system.scheduler
    implicit val ec: ExecutionContext = context.system.executionContext

    msg match {
      case _: Start                          =>
        cursorActor ! CursorManager.Get(cursorAdapter)
      case _: Stop                           =>
      case _: Suspend                        =>
      case _: Resume                         =>
      case _: UpdateCursor                   =>
        // TODO behavior changing to avoid cursor == null bug
        // then get rid of this nullable checking
        if (currentCursor != null) {
          cursorActor ! CursorManager.Update(currentCursor.ts, currentCursor.inc)
        }
      case Query(Key(db, coll, id), replyTo) =>
        val dumperKey = s"${db}.${coll}:${id}"
        dumperRefs.get(dumperKey) match {
          case Some(dumper) =>
            val getRs: Future[Dumper.Response] = dumper.ask(Dumper.Get)
            val doc = Await.result(getRs, 500.millis)
            replyTo ! doc
          case None         =>
            replyTo ! Dumper.NoData
        }
      case _@WrappedCursorResponse(resp)     =>
        val cursor = resp.asInstanceOf[CursorManager.Cursor]

        timers.startTimerAtFixedRate(UpdateCursor(), 1.second)

        val oplogs = init(BsonTimestamp(cursor.ts.toInt, cursor.inc), ops.toSeq)

        oplogs.subscribe(new Observer[Oplog] {
          override def onNext(oplog: Oplog): Unit = {
            currentCursor = CursorManager.Cursor(oplog.ts.getTime, oplog.ts.getInc)
            // a dumper is a long lived actor, it should watch by this context
            // and discovered by this context to get its state.
            val dumper = dumperRefs.getOrElseUpdate(oplog.key(), context.spawn(Dumper(oplog.key(), sourceDB), oplog.key()))
            // watch this dumper
            context.watch(dumper) // TODO listen dumper stop event

            dumper ! Dumper.Set(oplog)
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
      .withFilter(doc => nsFilter(doc))
      .filter(opsFilter)
      .map(trans)
      .filter(_.isDefined)
      .map(_.get)
      .observeOn(scala.concurrent.ExecutionContext.global)
  }

  private def trans(doc: Document): Option[Oplog] = {
    val op = doc.get("op").get.asInstanceOf[BsonString]
    val ns = doc.get("ns").get.asInstanceOf[BsonString]
    val ts = doc.get("ts").get.asInstanceOf[BsonTimestamp]
    val obj = doc.get("o").get.asInstanceOf[BsonDocument]

    doc.id match {
      case Right(id) =>
        Some(Oplog(id, op.getValue, ns.getValue, ts, obj))
      case Left(ex)  =>
        context.log.error("trans doc failed", ex)
        None
    }
  }

  private def opsFilter(doc: Document): Boolean = {
    ops.contains(doc.op)
  }

  private def nsFilter(doc: Document) = {
    doc.ns.startsWith("sdrc") // TODO fix this
  }
}

object Collector {

  def apply(oplogDB: MongoDatabase, sourceDB: MongoDatabase, ops: Set[String], cursorActor: ActorRef[CursorManager.Command]): Behavior[Command] =
    Behaviors.withTimers[Command](timers => {
      Behaviors.setup[Command](context => new Collector(oplogDB, sourceDB, ops, timers, cursorActor, context))
    })


  sealed trait Command

  sealed trait Response

  case class Start() extends Command

  case class Stop() extends Command

  case class Suspend() extends Command

  case class Resume() extends Command

  case class Key(db: String, coll: String, id: String)

  case class Query(key: Key, replyTo: ActorRef[Dumper.Response]) extends Command

  private case class UpdateCursor() extends Command

  private case class WrappedCursorResponse(resp: CursorManager.Response) extends Command

  private case class WrappedDumperResponse(resp: Dumper.Response) extends Command

  private case class WrappedReceptionistResponse(list: Receptionist.Listing) extends Command

}
