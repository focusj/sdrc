import org.bson.BsonObjectId
import org.mongodb.scala.bson.collection.Document
import org.mongodb.scala.bson.{BsonDocument, BsonString, Document, ObjectId}

object Implicits {

  implicit class MongoDocumentImplicits(doc: Document) {
    def ns: String = {
      doc.get("ns").get.asInstanceOf[BsonString].getValue
    }

    def op: String = {
      doc.get("op").get.asInstanceOf[BsonString].getValue
    }

    def id: Either[IllegalArgumentException, ObjectId] = {
      doc.op match {
        case "u"       =>
          Right(doc.get("o2").get.asInstanceOf[BsonObjectId].getValue)
        case "d" | "i" =>
          Right(doc.get("o").get.asInstanceOf[BsonDocument].get("_id").asInstanceOf[BsonObjectId].getValue)
        case _ =>
          Left(new IllegalArgumentException("not support"))

      }
    }
  }

}
