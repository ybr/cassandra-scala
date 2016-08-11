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
import cassandra.Decoders._

object ProtoStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    val conn = new Connection(new InetSocketAddress("192.168.99.100", 32769))
    val r = for {
      keyspace <- conn.connect("proto")
      _ = println("keyspace = " + keyspace)
      result <- conn.stream("SELECT data FROM test LIMIT 10", One)
    } yield {
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
    _ = println("FRAME = " + frame)
    body <- frame.body.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right).run()
    _ = println("BODY = " + body)
  } yield ()

  def connect(keyspace: String): Future[String] = for {
    _ <- connect
    frame <- actorRef ? Request.query(s"USE ${keyspace}", One) map(_.asInstanceOf[Frame])
    body <- frame.body.toMat(Sink.foreach(println))(Keep.right).run()
    _ = println("BODY = " + body)
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
  } yield "toto experiment"

  def options(): Future[Map[String, List[String]]] = {
    for {
      frame <- actorRef ? Request.options map(_.asInstanceOf[Frame])
      body <- frame.body.toMat(Sink.fold(ByteString.empty)(_ ++ _))(Keep.right).run
    } yield body.fromBytes[Map[String, List[String]]]._1
  }

  def stream(query: String, cl: ConsistencyLevel): Future[Any] = for {
    frame <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[Frame])
    body <- frame.body.toMat(Sink.fold(ByteString.empty) { (prev, curr) =>
      println("prev = " + prev)
      println("curr = " + curr)
      prev ++ curr
    })(Keep.right).run()
    result <- {
      frame.header.opcode match {
        case Opcode.Result =>
          val (resultHeader, remaining) = body.fromBytes[ResultHeader]
          resultHeader match {
            case r: Rows => Future.successful(r)
            case _ => Future.failed(new IllegalStateException("Expected rows result"))
          }
        case Opcode.Error =>
          val ((errorCode, errorMessage), remaining) = body.fromBytes[(Int, String)]
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
                      .atop(FrameHeaderBidi.dump)
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