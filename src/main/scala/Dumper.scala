import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.mongodb.scala.model.Filters
import org.mongodb.scala.{Document, MongoDatabase}

import scala.concurrent.Await

class Dumper(db: MongoDatabase) {

  import Dumper._

  val commandHandler: (State, Command) => Effect[Event, State] = { (state, command) =>
    command match {
      case Set(oplog)   =>
        val findRs = coll(oplog.ns).find(Filters.eq("_id", oplog.id)).head()
        val doc = Await.result(findRs, Global.MONGO_QUERY_TIMEOUT)
        Effect.persist(Seted(doc))
      case Get(replyTo) =>
        Effect.none.thenReply(replyTo)(_ => Doc(state.doc))
    }
  }

  val eventHandler: (State, Event) => State = { (state, event) =>
    event match {
      case Seted(doc) => state.copy(doc = doc)
    }
  }

  private def coll(ns: String) = {
    val i = ns.indexOf(".")
    val coll = ns.substring(i + 1)
    db.getCollection(coll)
  }

}

object Dumper {
  //Behaviors.setup[Dumper.Command](context => new Dumper(db, context))
  def apply(id: String, db: MongoDatabase): Behavior[Command] = {
    val dumper = new Dumper(db)

    EventSourcedBehavior(
      persistenceId = PersistenceId.ofUniqueId(id),
      emptyState = State(null),
      commandHandler = dumper.commandHandler,
      eventHandler = dumper.eventHandler
    )
  }

  sealed trait Command

  sealed trait Response

  sealed trait Event

  case class Set(oplog: Oplog) extends Command

  case class Seted(doc: Document) extends Event with Serializable

  case class Get(replyTo: ActorRef[Response]) extends Command

  case class Doc(doc: Document) extends Response

  case class State(doc: Document)

  case object NoData extends Response

}
