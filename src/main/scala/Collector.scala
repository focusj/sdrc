import Implicits.MongoDocumentImplicits
import akka.actor.typed.receptionist.Receptionist
import akka.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import akka.actor.typed.{ActorRef, Behavior, Scheduler}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import akka.util.Timeout
import com.mongodb.CursorType
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.model.Filters.and
import org.mongodb.scala.{MongoDatabase, Observable, Observer}

import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._


class Collector(
  oplogDB: MongoDatabase,
  sourceDB: MongoDatabase,
  ops: Set[String],
  timers: TimerScheduler[Collector.Command],
  cursorActor: ActorRef[CursorManager.Command],
  context: ActorContext[Collector.Command]
) {

  import Collector._

  val commandHandler: (State, Command) => Effect[Event, State] = { (state, command) =>
    val cursorAdapter: ActorRef[CursorManager.Response] =
      context.messageAdapter(msg => WrappedCursorResponse(msg))

    implicit val timeout: Timeout = 500.millis
    implicit val scheduler: Scheduler = context.system.scheduler
    implicit val ec: ExecutionContext = context.system.executionContext

    command match {
      case Start                             =>
        cursorActor ! CursorManager.Get(cursorAdapter)
        Effect.none
      case Stop                              =>
        Effect.stop()
      case AddDumper(key, ns, id)            =>
        Effect.persist(DumperAdded(key, ns, id)).thenRun(state => {
          state.dumpers.get(key).map(dumper => {
            context.watch(dumper)
            dumper ! Dumper.Set(ns, id)
          })
        })
      case UpdateCursor                      =>
        if (currentCursor != null) {
          cursorActor ! CursorManager.Update(currentCursor.ts, currentCursor.inc)
        }
        Effect.none
      case Query(Key(db, coll, id), replyTo) =>
        val dumperKey = s"${db}.${coll}:${id}"
        state.dumpers.get(dumperKey) match {
          case Some(dumper) => dumper ! Dumper.Get(replyTo)
          case None         => replyTo ! Dumper.NoData
        }
        Effect.none
      case _@WrappedCursorResponse(resp)     =>
        val cursor = resp.asInstanceOf[CursorManager.Cursor]

        timers.startTimerAtFixedRate(UpdateCursor, 1.second)

        val oplogs = init(BsonTimestamp(cursor.ts.toInt, cursor.inc), ops.toSeq)

        oplogs.subscribe(new Observer[Oplog] {
          override def onNext(oplog: Oplog): Unit = {
            currentCursor = CursorManager.Cursor(oplog.ts.getTime, oplog.ts.getInc)
            context.self ! AddDumper(oplog.key(), oplog.ns, oplog.id)
          }

          override def onError(e: Throwable): Unit = {
            context.log.error("{}", e)
          }

          override def onComplete(): Unit = {
            context.log.info("oplog collector done")
          }
        })
        Effect.none
    }
  }

  val eventHandler: (State, Event) => State = { (state, event) =>
    event match {
      case DumperAdded(key, ns, id) =>
        state.dumpers.getOrElseUpdate(key, context.spawn(Dumper(key, sourceDB), key))
        state
    }
  }

  private val coll = oplogDB.getCollection("oplog.rs")
  private var currentCursor: CursorManager.Cursor = _

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
        Some(Oplog(id.toHexString, op.getValue, ns.getValue, ts, obj))
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

  def apply(oplogDB: MongoDatabase, sourceDB: MongoDatabase, ops: Set[String], cursorActor: ActorRef[CursorManager.Command]): Behavior[Command] = {

    Behaviors.withTimers[Command](timers => {
      Behaviors.setup[Command](context => {
        val collector = new Collector(oplogDB, sourceDB, ops, timers, cursorActor, context)
        EventSourcedBehavior(
          persistenceId = PersistenceId.ofUniqueId("collector"),
          emptyState = State(mutable.HashMap.empty[String, ActorRef[Dumper.Command]]),
          commandHandler = collector.commandHandler,
          eventHandler = collector.eventHandler
        )
      })
    })

  }

  sealed trait Command

  sealed trait Response

  sealed trait Event extends Serializable

  case class Oplog(id: String, op: String, ns: String, ts: BsonTimestamp, doc: BsonDocument) {
    def key(): String = {
      ns + ":" + id
    }
  }


  case class Key(db: String, coll: String, id: String)

  case class State(dumpers: mutable.HashMap[String, ActorRef[Dumper.Command]])

  case class DumperAdded(key: String, ns: String, id: String) extends Event

  case class AddDumper(key: String, ns: String, id: String) extends Command

  case class Query(key: Key, replyTo: ActorRef[Dumper.Response]) extends Command

  private case class WrappedCursorResponse(resp: CursorManager.Response) extends Command

  private case class WrappedDumperResponse(resp: Dumper.Response) extends Command

  private case class WrappedReceptionistResponse(list: Receptionist.Listing) extends Command

  case object Start extends Command

  case object Stop extends Command

  private case object UpdateCursor extends Command


}
