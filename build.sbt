name := """cassandra-scala-driver"""

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.8",
  "com.typesafe.akka" %% "akka-stream" % "2.4.8",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)