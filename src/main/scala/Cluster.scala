package com.github.ybr.cassandra

import cassandra._

import akka.actor._
import akka.stream.scaladsl._
import akka.pattern._
import akka.util.Timeout

import java.net.InetSocketAddress

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

trait Cluster {
  def connect()(implicit ec: ExecutionContext, system: ActorSystem): Future[Session]
  def connect(keyspace: String)(implicit ec: ExecutionContext, system: ActorSystem): Future[Session]
}

class SimpleCluster(hosts: String*) extends Cluster {
  def connect()(implicit ec: ExecutionContext, system: ActorSystem): Future[Session] = Future.successful(new SimpleSession(this, None, hosts.toList, system))
  def connect(keyspace: String)(implicit ec: ExecutionContext, system: ActorSystem): Future[Session] = {
    val session = new SimpleSession(this, Some(keyspace), hosts.toList, system)
    session.connect().map(_ => session)
  }
}

object Cluster {
  def apply(hosts: String*): Cluster = new SimpleCluster(hosts: _*)
}