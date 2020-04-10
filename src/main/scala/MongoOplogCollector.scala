import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import com.mongodb.CursorType
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp}
import org.mongodb.scala.model.Filters.{and, exists, gt, in}
import org.mongodb.scala.{MongoDatabase, Observable, Observer}

class MongoOplogCollector(
  db: MongoDatabase,
  cursorActor: ActorRef[CursorManager.Command],
  context: ActorContext[MongoOplogCollector.Command]
)
  extends AbstractBehavior[MongoOplogCollector.Command](context) {

  import MongoOplogCollector._

  private val coll = db.getCollection("oplog.rs")

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
      case _@WrappedResponse(resp) =>
        val cursor = resp.asInstanceOf[CursorManager.Cursor]
        val oplogs = init(BsonTimestamp(cursor.ts.toInt, cursor.inc), Seq("i", "u", "d"))
        oplogs.subscribe(new Observer[Oplog] {
          override def onNext(result: Oplog): Unit = {
            context.log.info("{}", result)
          }

          override def onError(e: Throwable): Unit = {
            context.log.error("{}", e)
          }

          override def onComplete(): Unit = {
            context.log.info("consuming oplog done")
          }

        })
    }

    this
  }


  def init(from: BsonTimestamp, ops: Seq[String]): Observable[Oplog] = {
    val query = and(
      gt("ts", from),
      in("op", ops: _*),
      exists("fromMigrate", exists = false)
    )
    coll
      .find(query)
      .oplogReplay(true)
      .cursorType(CursorType.Tailable)
      .noCursorTimeout(true)
      .map(trans)
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
  private def trans(doc: Document) = {
    val op = doc.get("op").get.asInstanceOf[BsonString]
    val ns = doc.get("ns").get.asInstanceOf[BsonString]
    val ts = doc.get("ts").get.asInstanceOf[BsonTimestamp]
    val set = doc.get("o").get.asInstanceOf[BsonDocument]

    Oplog(op.getValue, ns.getValue, ts.getValue, set)
  }
}

object MongoOplogCollector {

  sealed trait Command

  case class Start() extends Command

  case class Stop() extends Command

  case class Suspend() extends Command

  case class Resume() extends Command

  case class WrappedResponse(resp: CursorManager.Response) extends Command

  def apply(db: MongoDatabase, cursorActor: ActorRef[CursorManager.Command]):Behavior[Command] =
    Behaviors.setup[Command](context => new MongoOplogCollector(db, cursorActor, context))

}
