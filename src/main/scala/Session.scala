package com.github.ybr.cassandra

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import akka.util.ByteString

import cassandra.streams.{Complete, Partial}

import java.net.InetSocketAddress

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Random

trait Session {
  def close()(implicit ec: ExecutionContext): Future[Unit]
  def execute(cl: ConsistencyLevel)(query: String, values: AnyRef*)(implicit ec: ExecutionContext): Future[ResultSource]
  def cluster(): Cluster
  def keyspace(): Option[String]
  def hosts(): List[String]
}

class SimpleSession(
  val cluster: Cluster,
  val keyspace: Option[String],
  val hosts: List[String],
  actorSystem: ActorSystem
) extends Session {
  val address = "(.+):(.+)".r
  val random = new Random()

  val connections: List[Connection] = hosts.map {
    case address(host, port) => new Connection(new InetSocketAddress(host, port.toInt))(actorSystem)
    case malformed => throw new IllegalArgumentException(s"${malformed} is not a host:port")
  }

  def connect()(implicit ec: ExecutionContext): Future[Unit] = Future.sequence(connections.map { conn =>
    keyspace match {
      case Some(ks) => conn.connect(ks)
      case None => conn.connect()
    }
  }).map(_ => ())

  def close()(implicit ec: ExecutionContext): Future[Unit] = ???

  def execute(cl: ConsistencyLevel)(query: String, values: AnyRef*)(implicit ec: ExecutionContext): Future[ResultSource] = {
    val conn = connections(random.nextInt() % connections.length)
    println("VALUES " + values)
    conn.stream(cl)(query, values).map {
      case Partial((fh, fb), source) => ResultSource(fh, fb, source)
      case Complete((fh, fb), bytes) => ResultSource(fh, fb, Source.single(bytes))
    }
  }
}