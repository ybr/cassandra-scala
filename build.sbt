name := """cassandra-scala-driver"""

version := "0.1-SNAPSHOT"

scalaVersion := "2.11.7"

crossScalaVersions := Seq("2.10.6")

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.9",
  "com.typesafe.akka" %% "akka-stream" % "2.4.9",
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0",
  "org.scalatest" %% "scalatest" % "3.0.0" % "test"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:implicitConversions",
  "-language:higherKinds",
  "-language:existentials",
  "-language:postfixOps"
)