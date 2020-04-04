import java.time.Instant

import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.model.FindOneAndReplaceOptions

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.Future

trait CursorManager {
  this: Mongoable =>

  private val coll = db().getCollection("cursor")

  def get: Future[BsonTimestamp] = {
    coll.find(Document("_id" -> 1)).headOption().map {
      case Some(v) =>
        BsonTimestamp(v.getInteger("time"), v.getInteger("inc"))
      case None    =>
        BsonTimestamp(Instant.now().getEpochSecond.toInt, 0)

    }
  }

  def update(time: Int, inc: Int): Unit = {
    val opt = FindOneAndReplaceOptions().upsert(true)
    coll.findOneAndReplace(Document("_id" -> 1), Document("time" -> time, "inc" -> inc), opt)
  }
}
