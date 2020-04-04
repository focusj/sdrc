import com.mongodb.CursorType
import org.mongodb.scala.Observable
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, BsonTimestamp}
import org.mongodb.scala.model.Filters.{and, exists, gt, in}


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
