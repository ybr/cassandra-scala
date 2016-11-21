package com.github.ybr.cassandra

import akka.NotUsed
// import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.actor.ActorSystem
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}

import cassandra.decoder.{CassandraDecoders, Consumed}
import cassandra._
import cassandra.protocol._
import cassandra.streams.{Complete, DetachResult, Partial, StreamDetacher}

import java.net.InetSocketAddress
import java.nio.ByteOrder

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

class Connection(remote: InetSocketAddress)(implicit system: ActorSystem) {
  implicit val timeout = new Timeout(10 second)

  implicit val materializer = ActorMaterializer.create(system)

  val actorRef = system.actorOf(ConnectionActor.props(remote))

  def connect()(implicit ec: ExecutionContext): Future[Unit] = for {
    _ <- actorRef ? Request.startup
  } yield ()

  def connect(keyspace: String)(implicit ec: ExecutionContext): Future[String] = for {
    _ <- connect
    result <- actorRef ? Request.query(s"USE ${keyspace}", One, Seq.empty) map(_.asInstanceOf[DetachResult[(FrameHeader, FrameBody)]])
  } yield {
    val Complete((fh, fb), _) = result
    val Result(_, SetKeyspace(ks)) = fb
    ks
  }

  def options()(implicit ec: ExecutionContext): Future[Map[String, List[String]]] = {
    for {
      result <- actorRef ? Request.options map(_.asInstanceOf[DetachResult[(FrameHeader, FrameBody)]])
      body <- result match {
        case Complete(_ , body) => Future.successful(body)
        case Partial(_, source) => source.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right).run
      }
    } yield CassandraDecoders.multimap.decode(body) match {
      case Consumed(multimap, _) => multimap
    }
  }

  def stream(cl: ConsistencyLevel)(query: String, params: Seq[AnyRef])(implicit ec: ExecutionContext): Future[DetachResult[(FrameHeader, FrameBody)]] = {
    actorRef ? Request.query(query, cl, params) map(_.asInstanceOf[DetachResult[(FrameHeader, FrameBody)]])
    // } yield result match 
    //   case Complete((fh, fb), bytes) => (fh, fb, Source.single(bytes))
    //   case Partial((fh, fb), source) => (fh, fb, source)
    // }
  }
}