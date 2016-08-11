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
  def connect(): Future[Session]
  def close(): Unit
  def options(): Future[Map[String, Seq[String]]]
  def connect(keyspace: String): Future[Session]
}

object Cluster {
  // def apply(host: String): Cluster = {
  //   val addrport = "(.+):(.+)".r
  //   val address = host match {
  //     case addrport(addr, port) => new InetSocketAddress(addr, port.toInt)
  //     case malformed => throw new IllegalArgumentException(s"${malformed} is not a host:port")
  //   }
  //   new ActorBasedCluster(address)
  // }
}

// class ActorBasedCluster(host: InetSocketAddress) extends Cluster {
//   private implicit val system = ActorSystem()

//   // implicit val timeout = new Timeout(10 seconds)

//   // private val client = system.actorOf(cassandra.Client.props(host))

//   def connect(): Future[Session] = {
//     val f = Promise[Session]
//     // Tcp().outgoingConnection(host)
//     // import akka.io.{IO, Tcp}
//     // import Tcp._

//     // IO(Tcp) ? Connect(host) map { c =>
//     //   println(c)
//     //   new Session {}
//     // }
//     // client ? ConnectIt map { r =>
//     //   println(r)
//     //   new Session {}
//     // }
//     f.future
//   }

//   def close() {
//     // system.shutdown()
//   }
//   // def connect(keyspace: String): Future[Session]
// }

trait Session {

}