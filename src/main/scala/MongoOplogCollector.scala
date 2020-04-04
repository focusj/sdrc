import com.mongodb.CursorType
import org.mongodb.scala.Observable
import org.mongodb.scala.bson.BsonTimestamp
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.model.Filters.{and, exists, gt, in}

import scala.collection.JavaConverters._


trait MongoOplogCollector {
  this: Mongoable with Configurable =>

  val cursorManager: CursorManager

  private val coll = db().getCollection("oplog.rs")

  def run(from: BsonTimestamp, ops: Seq[String]): Observable[Oplog] = {
    val query = and(
      gt("ts", from),
      in("op", ops: _*),
      exists("fromMigrate", exists = false)
    )
    coll.find(query)
        .oplogReplay(true)
        .cursorType(CursorType.Tailable)
        .noCursorTimeout(true)
        .map(trans)
  }

  private def trans(doc: Document) = {
    print(doc)
    Oplog("op")
  }
}
