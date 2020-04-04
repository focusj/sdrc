import org.mongodb.scala.{MongoClient, MongoDatabase}

trait Mongoable {
  val uri: String
  val database: String

  def db(): MongoDatabase = client().getDatabase(database)

  def client(): MongoClient = MongoClient(uri)
}
