import Dumper.Doc
import akka.actor.typed.receptionist.ServiceKey
import akka.actor.typed.scaladsl.{AbstractBehavior, ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.{Document, MongoDatabase}

import scala.concurrent.Await

class Dumper(
  db: MongoDatabase,
  context: ActorContext[Dumper.Command]
) extends AbstractBehavior[Dumper.Command](context) {
  private var state: Document = _

  override def onMessage(msg: Dumper.Command): Behavior[Dumper.Command] = {
    msg match {
      case Dumper.Set(oplog)   =>
        val findRs = coll(oplog.ns).find(Filters.eq("_id", oplog.id)).head()
        state = Await.result(findRs, Global.MONGO_QUERY_TIMEOUT)
      case Dumper.Get(replyTo) =>
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

object Dumper {

  def DumperServiceKey(name: String) = ServiceKey[Command](name)

  def apply(db: MongoDatabase) = Behaviors.setup[Dumper.Command](context => new Dumper(db, context))

  sealed trait Command

  sealed trait Response

  case class Set(oplog: Oplog) extends Command

  case class Get(replyTo: ActorRef[Response]) extends Command

  case class Doc(doc: Document) extends Response

  case object NoData extends Response

}
