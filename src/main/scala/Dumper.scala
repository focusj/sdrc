import akka.actor.typed.scaladsl.{ActorContext, Behaviors}
import akka.actor.typed.{ActorRef, Behavior}
import akka.persistence.typed.PersistenceId
import akka.persistence.typed.scaladsl.{Effect, EventSourcedBehavior}
import org.mongodb.scala.MongoDatabase
import org.mongodb.scala.bson.ObjectId
import org.mongodb.scala.model.Filters

import scala.concurrent.Await

class Dumper(db: MongoDatabase, context: ActorContext[Dumper.Command]) {

  import Dumper._

  val commandHandler: (State, Command) => Effect[Event, State] = { (state, command) =>
    command match {
      case Set(ns, id)  =>
        val findRs = coll(ns).find(Filters.eq("_id", new ObjectId(id))).head()
        val doc = Await.result(findRs, Global.MONGO_QUERY_TIMEOUT)
        Effect.persist(Seted(doc.toJson()))
      case Get(replyTo) =>
        Effect.none.thenReply(replyTo)(_ => Doc(state.json))
    }
  }

  val eventHandler: (State, Event) => State = { (state, event) =>
    event match {
      case Seted(json) => state.copy(json = json)
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
    Behaviors.setup(context => {
      val dumper = new Dumper(db, context)

      EventSourcedBehavior(
        persistenceId = PersistenceId.ofUniqueId(id),
        emptyState = State(null),
        commandHandler = dumper.commandHandler,
        eventHandler = dumper.eventHandler
      )
    })

  }

  sealed trait Command

  sealed trait Response

  sealed trait Event extends Serializable

  case class Set(ns: String, id: String) extends Command

  case class Seted(json: String) extends Event

  case class Get(replyTo: ActorRef[Response]) extends Command

  case class Doc(json: String) extends Response

  case class State(json: String) extends Serializable

  case object NoData extends Response

}
