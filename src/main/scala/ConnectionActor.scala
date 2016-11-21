package com.github.ybr.cassandra

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.stream.{Fusing, Materializer, OverflowStrategy}
import akka.stream.scaladsl.{Flow, Sink, Source, Tcp}

import cassandra.protocol._
import cassandra.streams.{Complete, DetachResult, FrameHeaderBidi, Partial}

import java.net.InetSocketAddress

class ConnectionActor(remote: InetSocketAddress)(implicit system: ActorSystem, materializer: Materializer) extends Actor {
  var listenerRef: Option[ActorRef] = None

  val notifyListener = Flow[DetachResult[(FrameHeader, FrameBody)]].map { f =>
    self ! f
    f
  }

  val runnable = Source.actorRef(1, OverflowStrategy.fail) // here might be here why it fails
                  .via(
                    Fusing.aggressive(
                      // FrameBodyBidi.framing
                      // .atop(
                        FrameHeaderBidi.framing
                        // )
                      // .atop(BenchBidi.dump)
                      // dump IO
                      // .atop(cassandra.streams.TcpDumpBidi.dump)
                      // .atop(BenchBidi.dump)
                      // buffer IO
                      // .atop(BidiFlow.fromFlows(
                      //   Flow[ByteString].buffer(10, OverflowStrategy.backpressure),
                      //   Flow[ByteString].buffer(10, OverflowStrategy.backpressure)
                      // ))
                      .join(Tcp().outgoingConnection(remote))
                  ))
                  .via(notifyListener)
                  .to(Sink.ignore)
  val tcpActor: ActorRef = runnable.run()

  def receive = {
    case frame @ (FrameHeader(version, _, _, _, _), source) if version == 0x04 => // request
      listenerRef = Some(sender)
      tcpActor ! frame

    case frame @ Complete((fh, fb), bytes)  =>
      listenerRef.foreach(_ ! frame)
      listenerRef = None

    case frame @ Partial((fh, fb), bytes)  =>
      listenerRef.foreach(_ ! frame)
      listenerRef = None

    case unhandled =>
      listenerRef.foreach(_ ! Status.Failure(new IllegalArgumentException("Not handled")))
      listenerRef = None
  }
}

object ConnectionActor {
  def props(remote: InetSocketAddress)(implicit system: ActorSystem, materializer: Materializer) = Props(classOf[ConnectionActor], remote, system, materializer)
}