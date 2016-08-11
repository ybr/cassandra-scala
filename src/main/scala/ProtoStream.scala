package cassandra

import akka.actor.{Actor, ActorRef, ActorSystem, Props, Status}
import akka.pattern.ask
import akka.stream._
import akka.stream.scaladsl._

import akka.util.{ByteString, Timeout}

import java.net.InetSocketAddress

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

import cassandra.protocol._
import cassandra.Encoders._
import cassandra.decoder._

object ProtoStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    val conn = new Connection(new InetSocketAddress("192.168.99.100", 32769))
    val r = for {
      keyspace <- conn.connect("proto")
      _ = println("------------------------- START = " + System.currentTimeMillis)
      result <- conn.stream("SELECT data FROM test LIMIT 1500", One)
      // _ <- conn.stream("SELECT data FROM test LIMIT 10", One)
    } yield {
      println("------------------------- END = " + System.currentTimeMillis)
      println("result " + result)
    }
    r
    .recoverWith {
      case t =>
        println("ERROR TOTO " + t.getMessage)
        Future(())
    }

    Thread.sleep(1000)
    system.shutdown()
    println("END")
  }
}

class Connection(remote: InetSocketAddress)(implicit system: ActorSystem) {
  implicit val timeout = new Timeout(10 second)

  implicit val materializer = ActorMaterializer.create(system)

  val actorRef = system.actorOf(ConnectionActor.props(remote))

  def connect(): Future[Unit] = for {
    frame <- actorRef ? Request.startup map(_.asInstanceOf[Frame])
    body <- frame.body.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right).run()
  } yield ()

  def connect(keyspace: String): Future[String] = for {
    _ <- connect
    frame <- actorRef ? Request.query(s"USE ${keyspace}", One) map(_.asInstanceOf[Frame])
    body <- frame.body.runWith(Sink.fold(ByteString.empty)(_ ++ _))
    // result <- {
    //   frame.header.opcode match {
    //     case Opcode.Result =>
    //       val (resultHeader, remaining) = body.fromBytes[ResultHeader]
    //       resultHeader match {
    //         case SetKeyspace(keyspace) => Future.successful(keyspace)
    //         case _ => Future.failed(new IllegalStateException("Expected SetKeyspace result"))
    //       }
    //     case Opcode.Error =>
    //       val ((errorCode, errorMessage), remaining) = body.fromBytes[(Int, String)]
    //       Future.failed(new RuntimeException(s"(${errorCode}) ${errorMessage}"))
    //     case opcode => Future.failed(new IllegalStateException(s"The opcode is not one correct, expected SetKeyspace actual ${opcode}"))
    //   }
    // }
  } yield {
    val Consumed(SetKeyspace(ks), _, _) = CassandraDecoders.resultHeader.decode(body)
    ks
  }

  def options(): Future[Map[String, List[String]]] = {
    for {
      frame <- actorRef ? Request.options map(_.asInstanceOf[Frame])
      body <- frame.body.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right).run
    } yield CassandraDecoders.multimap.decode(body) match {
      case Consumed(multimap, _, _) => multimap
    }
  }

  def stream(query: String, cl: ConsistencyLevel): Future[Any] = for {
    frame <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[Frame])
    body <- frame.body.runWith(Sink.fold(ByteString.empty)(_ ++ _))
    result <- {
      frame.header.opcode match {
        case Opcode.Result =>
          val Consumed(resultHeader, remaining, _) = CassandraDecoders.resultHeader.decode(body)
          resultHeader match {
            case r: Rows => Future.successful(r)
            case _ => Future.failed(new IllegalStateException("Expected rows result"))
          }
        case Opcode.Error =>
          val deco = for {
            int <- CassandraDecoders.int
            string <- CassandraDecoders.string
          } yield (int, string)
          val Consumed((errorCode, errorMessage), remaining, _) = deco.decode(body)
          Future.failed(new RuntimeException(s"(${errorCode}) ${errorMessage}"))
        case opcode => Future.failed(new IllegalStateException(s"The opcode is not one of expected ${opcode}"))
      }
    }
  } yield result
}

class ConnectionActor(remote: InetSocketAddress)(implicit system: ActorSystem) extends Actor {
  implicit val materializer = ActorMaterializer.create(system)

  var listenerRef: Option[ActorRef] = None

  val notifyListener = Flow[Frame].map { f =>
    self ! f
    f
  }

  val runnable = Source.actorRef(1, OverflowStrategy.fail)
                  .via(
                    FrameHeaderBidi.framing
                      // .atop(FrameHeaderBidi.dump)
                      .join(Tcp().outgoingConnection(remote))
                  )
                  .via(notifyListener)
                  .to(Sink.ignore)
  val tcpActor: ActorRef = runnable.run()

  def receive = {
    case frame: Frame if frame.header.version == 0x04 => // request
      listenerRef = Some(sender)
      tcpActor ! frame
    case frame: Frame if frame.header.version == -124 => // response
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