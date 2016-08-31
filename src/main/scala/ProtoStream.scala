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
import scala.util._

object ProtoStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    implicit val materializer = ActorMaterializer.create(system)

    val conn = new Connection(new InetSocketAddress("192.168.99.100", 32769))
    val r: Future[ResultSource] = for {
      keyspace <- conn.connect("proto")
      start = System.currentTimeMillis
      (fh, fb, columns) <- conn.stream("SELECT data FROM test LIMIT 1", One)
      _ = println("Columns count = " + fb.asInstanceOf[Result].header.asInstanceOf[Rows].columnsCount)
      rows = columns.via(Flow.fromGraph(new Grouped[Column](fb.asInstanceOf[Result].header.asInstanceOf[Rows].columnsCount))).map(Row(_))
    } yield {
      val end = System.currentTimeMillis
      println(s"------------------------- Duration = ${end - start} (ms)")
      println(fh)
      println(fb)
      ResultSource(rows)
    }

    val r1 = r.flatMap { result =>
      println("RESULT")
      result.rows.mapAsync(1) { row =>
        // println("ROW")
        row.columns.mapAsync(1) { column =>
          // println("\tCOLUMN")
          column.content.mapAsync(1) { bs =>
            // println("\t\tBYTES " + bs)
            Future.successful(bs)
          }
          .runWith(Sink.seq)
        }
        .runWith(Sink.seq)
      }
      .runWith(Sink.seq)
    }

    val start = System.currentTimeMillis
    val t = Try(Await.result(r1.recoverWith {
      case t =>
        println("ERROR TOTO " + t.getMessage)
        t.printStackTrace
        Future(())
    }, 50 second))
    val end = System.currentTimeMillis
    println(s"Duration: ${end - start}(ms)")

    t match {
      case Success(v) => println("OK " + v.asInstanceOf[Vector[Vector[Vector[ByteString]]]].size)
      case Failure(t) => t.printStackTrace
    }

    system.shutdown()
    println("END")
  }
}

class Connection(remote: InetSocketAddress)(implicit system: ActorSystem) {
  implicit val timeout = new Timeout(10 second)

  implicit val materializer = ActorMaterializer.create(system)

  val actorRef = system.actorOf(ConnectionActor.props(remote))

  def connect(): Future[Unit] = for {
    _ <- actorRef ? Request.startup
  } yield ()

  def connect(keyspace: String): Future[String] = for {
    _ <- connect
    (fh, fb, source) <- actorRef ? Request.query(s"USE ${keyspace}", One) map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
  } yield {
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

  def stream(query: String, cl: ConsistencyLevel): Future[(FrameHeader, FrameBody, Source[Column, NotUsed])] = for {
    (fh, fb, source) <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
    rowsHeader = fb.asInstanceOf[Result].header.asInstanceOf[Rows]
    result = source
                  .via(Flow.fromGraph(new StreamDetacher(CassandraDecoders.int.more(identity)))
                  .map(t => Column(t._2)))
  } yield (fh, fb, result)
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
                      // dump IO
                      // .atop(TcpDumpBidi.dump)
                      // buffer IO
                      // .atop(BidiFlow.fromFlows(
                      //   Flow[ByteString].buffer(10, OverflowStrategy.backpressure),
                      //   Flow[ByteString].buffer(10, OverflowStrategy.backpressure)
                      // ))
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