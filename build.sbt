name := "sdrc"

version := "0.1"

scalaVersion := "2.12.8"

val akkaVersion = "2.6.4"
val akkaHttpVersion = "10.1.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed"   % akkaVersion,
  "com.typesafe.akka" %% "akka-stream"        % akkaVersion,
  "com.typesafe.akka" %% "akka-http"          % akkaHttpVersion,
  "org.mongodb.scala" %% "mongo-scala-driver" % "2.9.0",
  "org.apache.kafka"  %% "kafka"              % "2.3.0",
  "org.slf4j"         %  "slf4j-api"          % "1.7.28",
  "org.slf4j"         %  "slf4j-simple"       % "1.7.28",
  "ch.qos.logback"    %  "logback-classic"    % "1.2.3",
)