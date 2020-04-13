import OplogDumper.Doc
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.{Document, MongoDatabase}

import scala.concurrent.Await
import scala.concurrent.duration._

class OplogDumper(
  db: MongoDatabase,
  context: ActorContext[OplogDumper.Command]
) extends AbstractBehavior[OplogDumper.Command](context) {
  private var state: Document = _

  override def onMessage(msg: OplogDumper.Command): Behavior[OplogDumper.Command] = {
    msg match {
      case OplogDumper.Set(oplog)   =>
        val findRs = coll(oplog.ns).find(Filters.eq("_id", oplog.id)).head()
        state = Await.result(findRs, 100.millis)
      case OplogDumper.Get(replyTo) =>
        replyTo ! Doc(state)
    }

    this
  }

  def coll(ns: String) = {
    val i = ns.indexOf(".")
    val coll = ns.substring(i + 1)
    db.getCollection(coll)
  }

}

object OplogDumper {

  def DumperServiceKey(name: String) = ServiceKey[Command](name)

  def apply(db: MongoDatabase) = Behaviors.setup[OplogDumper.Command](context => new OplogDumper(db, context))

  sealed trait Command

  sealed trait Response

  case class Set(oplog: Oplog) extends Command

  case class Get(replyTo: ActorRef[Response]) extends Command

  case class Doc(doc: Document) extends Response

}
