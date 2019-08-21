name := "sdrc"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.23",
  "com.typesafe.akka" %% "akka-stream" % "2.5.23",
  "com.lightbend.akka" %% "akka-stream-alpakka-mongodb" % "1.1.0",
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0",
  "com.typesafe.akka" %% "akka-http"   % "10.1.9"
)