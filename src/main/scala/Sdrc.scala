import Collector.Key
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import akka.actor.typed.{ActorRef, ActorSystem, Behavior, PostStop}
import akka.http.scaladsl.Http
import akka.http.scaladsl.Http.ServerBinding
import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives.{Segments, complete, get, path, _}
import akka.http.scaladsl.server.Route
import akka.stream.Materializer
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory}
import org.mongodb.scala.bson.{BsonDocument, BsonTimestamp, ObjectId}
import org.mongodb.scala.{MongoClient, MongoDatabase}

import scala.collection.JavaConverters._
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


trait Configurable {
  def config(): Config = ConfigFactory.load()
}

class SdrcRoutes(dataActor: ActorRef[Collector.Command])(implicit system: ActorSystem[_]) {

  import akka.actor.typed.scaladsl.AskPattern._

  implicit val timeout: Timeout = 1000.millis

  val route: Route =
    path(Segments) {
      case List(db, coll, id) =>
        get {
          val queryRs: Future[Dumper.Response] = dataActor.ask(Collector.Query(Key(db, coll, id), _))
          onSuccess(queryRs) {
            case Dumper.Doc(data) =>
              complete(HttpEntity(ContentTypes.`application/json`, data))
            case Dumper.NoData    =>
              complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"not found data for: [$db/$coll/$id]"))
          }
        }
      case _                  =>
        get {
          complete(HttpEntity(ContentTypes.`text/plain(UTF-8)`, s"invalid path"))
        }
    }
}

case class OplogConf(after: BsonTimestamp, ops: List[String])

case class Oplog(id: ObjectId, op: String, ns: String, ts: BsonTimestamp, doc: BsonDocument) {
  def key(): String = {
    ns + ":" + id.toHexString
  }
}

object Sdrc extends Configurable {

  def main(args: Array[String]) {
    val system: ActorSystem[Sdrc.Command] =
      ActorSystem(Sdrc("localhost", 8080), "SdrcHttpServer")

    //    val latch = new CountDownLatch(1)
    //    latch.await()
    //    system.terminate()
  }

  def apply(host: String, port: Int): Behavior[Command] = Behaviors.setup { ctx =>
    implicit val system: ActorSystem[Nothing] = ctx.system
    implicit val untypedSystem: akka.actor.ActorSystem = ctx.system.toClassic
    implicit val materializer: Materializer = Materializer(ctx.system.toClassic)
    implicit val ec: ExecutionContextExecutor = concurrent.ExecutionContext.global

    val cursorDb = getDb(config().getString("sdrc.cursor.mongo.uri"),
      config().getString("sdrc.cursor.mongo.database"))

    val oplogDb = getDb(config().getString("sdrc.collector.mongo.uri"),
      config().getString("sdrc.collector.mongo.database"))

    val sourceDb = getDb(config().getString("sdrc.collector.mongo.uri"), "sdrc")

    val cursorActor = ctx.spawn(CursorManager(cursorDb), "cursor-actor")

    val ops = config().getStringList("sdrc.collector.mongo.ops").asScala.toSet
    val oplogActor = ctx.spawn(Collector(oplogDb, sourceDb, ops, cursorActor), "oplog-actor")
    oplogActor ! Collector.Start()

    val serverBinding: Future[Http.ServerBinding] =
      Http.apply().bindAndHandle(new SdrcRoutes(oplogActor).route, host, port)

    ctx.pipeToSelf(serverBinding) {
      case Success(binding) => Started(binding)
      case Failure(ex)      => StartFailed(ex)
    }

    def running(binding: ServerBinding): Behavior[Command] =
      Behaviors.receiveMessagePartial[Command] {
        case Stop =>
          ctx.log.info(
            "Stopping server http://{}:{}/",
            binding.localAddress.getHostString,
            binding.localAddress.getPort)
          Behaviors.stopped
      }.receiveSignal {
        case (_, PostStop) =>
          binding.unbind()
          Behaviors.same
      }

    def starting(wasStopped: Boolean): Behaviors.Receive[Command] =
      Behaviors.receiveMessage[Command] {
        case StartFailed(cause) =>
          throw new RuntimeException("Server failed to start", cause)
        case Started(binding)   =>
          ctx.log.info(
            "Server online at http://{}:{}/",
            binding.localAddress.getHostString,
            binding.localAddress.getPort)
          if (wasStopped) ctx.self ! Stop
          running(binding)
        case Stop               =>
          // we got a stop message but haven't completed starting yet,
          // we cannot stop until starting has completed
          starting(wasStopped = true)
      }

    starting(wasStopped = false)
  }

  private def getDb(address: String, dbName: String): MongoDatabase = {
    MongoClient(address).getDatabase(dbName)
  }

  sealed trait Command

  private final case class StartFailed(cause: Throwable) extends Command

  private final case class Started(binding: ServerBinding) extends Command

  case object Stop extends Command

}