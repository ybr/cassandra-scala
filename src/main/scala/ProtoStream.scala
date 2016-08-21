package cassandra

import akka.NotUsed
import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}

import cassandra.Encoders._
import cassandra.decoder._
import cassandra.protocol._
import cassandra.streams._

import java.net.InetSocketAddress

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

object ProtoStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    val conn = new Connection(new InetSocketAddress("192.168.99.100", 32769))
    val r = for {
      keyspace <- conn.connect("proto")
      _ = println("------------------------- START = " + System.currentTimeMillis)
      result <- conn.stream("SELECT data FROM test LIMIT 10", One)
      // _ <- conn.stream("SELECT data FROM test LIMIT 10", One)
    } yield {
      println("------------------------- END = " + System.currentTimeMillis)
      println("result " + result)
    }
    r
    .recoverWith {
      case t =>
        println("ERROR TOTO " + t.getMessage)
        t.printStackTrace
        Future(())
    }

    Thread.sleep(2000)
    system.shutdown()
    println("END")
  }
}

class Connection(remote: InetSocketAddress)(implicit system: ActorSystem) {
  implicit val timeout = new Timeout(10 second)

  implicit val materializer = ActorMaterializer.create(system)

  val actorRef = system.actorOf(ConnectionActor.props(remote))

  def connect(): Future[Unit] = for {
    (fh, fb, b) <- actorRef ? Request.startup map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, akka.NotUsed])])
    _ = println("Signet 1 " + fh)
    _ = println("Signet 1 " + fb)
    _ = {
      val t: Source[ByteString, NotUsed] = b
      println("Signet 1 " + b)
    }
    // empty source do not run it
    // body <- b.via(Flow[ByteString].map { bs =>
    //   println("Signet 3 " + bs)
    // }).runWith(Sink.seq)
  } yield {
    // println("Signet 2 " + body)
    println("Signet 2")
    ()
  }

  def connect(keyspace: String): Future[String] = for {
    _ <- connect
    _ = println("AFTER CONNECT")
    (fh, fb, source) <- actorRef ? Request.query(s"USE ${keyspace}", One) map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
    _ = println("Signet 1i " + fh + " " + fb + " " + source)
    // body <- source.runWith(Sink.fold(ByteString.empty)(_ ++ _))
  } yield {
    println("TOTOTOTOTOTOTOTO")
    val Result(_, SetKeyspace(ks)) = fb
    ks
  }

  def options(): Future[Map[String, List[String]]] = {
    for {
      (fh, fb, source) <- actorRef ? Request.options map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
      body <- source.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right).run
    } yield CassandraDecoders.multimap.decode(body) match {
      case Consumed(multimap, _, _) => multimap
    }
  }

  def stream(query: String, cl: ConsistencyLevel): Future[Any] = for {
    (fh, fb, source) <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
    body <- source.runWith(Sink.fold(ByteString.empty)(_ ++ _))
    result <- Future.successful(CassandraDecoders.frameBody(fh.opcode).decode(body)) // here
  } yield result
}

class ConnectionActor(remote: InetSocketAddress)(implicit system: ActorSystem) extends Actor {
  implicit val materializer = ActorMaterializer.create(system)

  var listenerRef: Option[ActorRef] = None

  val notifyListener = Flow[(FrameHeader, FrameBody, Source[ByteString, akka.NotUsed])].map { f =>
    self ! f
    f
  }

  val runnable = Source.actorRef(10, OverflowStrategy.fail) // here might be here why it fails
                  .via(
                      FrameBodyBidi.framing
                      .atop(FrameHeaderBidi.framing)
                      .atop(TcpDumpBidi.dump)
                      .join(Tcp().outgoingConnection(remote))
                  )
                  .via(notifyListener)
                  .to(Sink.ignore)
  val tcpActor: ActorRef = runnable.run()

  def receive = {
    case frame @ (FrameHeader(version, _, _, _, _), source) if version == 0x04 => // request
      listenerRef = Some(sender)
      tcpActor ! frame
    // case frame: Frame if frame.header.version == -124 => // response
    //   listenerRef.foreach(_ ! frame)
    //   listenerRef = None

    case frame @ (FrameHeader(version, _, _, _, _), fb, source) if version == -124 => // response
      listenerRef.foreach(_ ! frame)
      listenerRef = None

    case unhandled =>
      println("Unhandled " + unhandled)
      listenerRef.foreach(_ ! Status.Failure(new IllegalArgumentException("Not handled")))
      listenerRef = None
  }
}

object ConnectionActor {
  def props(remote: InetSocketAddress)(implicit system: ActorSystem) = Props(classOf[ConnectionActor], remote, system)
}