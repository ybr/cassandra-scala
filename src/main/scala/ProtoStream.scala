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
    val r1: Future[Unit] = for {
      keyspace <- conn.connect("proto")
      start = System.currentTimeMillis
      _ <- loop(10000)(Dao.get1(conn, materializer))
      end = System.currentTimeMillis
      _ = println(s"Total: ${end - start}(ms)")
    } yield ()

    Try(Await.result(r1.recoverWith {
      case t =>
        println("ERROR TOTO " + t.getMessage)
        t.printStackTrace
        Future(())
    }, 10 second)) match {
      case Success(v) => println("OK")
      case Failure(t) => t.printStackTrace
    }

    system.terminate()
    println("END")
  }

  def loop(n: Int)(f: => Future[Unit])(implicit ec: ExecutionContext): Future[Unit] = if(n == 0) Future.successful(()) else f.flatMap(_ => loop(n - 1)(f))
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
    result <- actorRef ? Request.query(s"USE ${keyspace}", One) map(_.asInstanceOf[DetachResult[(FrameHeader, FrameBody)]])
    _ = println(result)
  } yield {
    val Complete((fh, fb), _) = result
    val Result(_, SetKeyspace(ks)) = fb
    ks
  }

  def options(): Future[Map[String, List[String]]] = {
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

  val detacher  = Flow.fromGraph(new StreamDetacher(CassandraDecoders.int)(identity))
                  .map {
                    case Partial(_, source) => Column(source)
                    case Complete(_, bytes) => Column(Source.single(bytes))
                  }

  def stream(query: String, cl: ConsistencyLevel): Future[(FrameHeader, FrameBody, Source[Column, NotUsed])] = for {
    frame <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[DetachResult[(FrameHeader, FrameBody)]])
  } yield frame match {
    case Complete((fh, fb), bytes) =>
      val rowsHeader = fb.asInstanceOf[Result].header.asInstanceOf[Rows]
      val Consumed(columns, _) = CassandraDecoders.list(rowsHeader.rowsCount * rowsHeader.columnsCount)(CassandraDecoders.bytes.map(bytes => Column(Source.single(bytes)))).decode(bytes)
      (fh, fb, Source(columns))

    case Partial((fh, fb), source) =>
      (fh, fb, source.via(detacher))
  }

  def stream1(query: String, cl: ConsistencyLevel): Future[(FrameHeader, FrameBody, Source[ByteString, NotUsed])] = {
    for {
      result <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[DetachResult[(FrameHeader, FrameBody)]])
    } yield result match {
      case Complete((fh, fb), bytes) => (fh, fb, Source.single(bytes))
      case Partial((fh, fb), source) => (fh, fb, source)
    }
  }
}

class ConnectionActor(remote: InetSocketAddress)(implicit system: ActorSystem) extends Actor {
  implicit val materializer = ActorMaterializer.create(system)

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
                      // .atop(TcpDumpBidi.dump)
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
      // println("Complete")
      listenerRef.foreach(_ ! frame)
      listenerRef = None

    case frame @ Partial((fh, fb), bytes)  =>
      println("Partial " + fh)
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

object Dao {
  def get(implicit conn: Connection, mat: Materializer): Future[Unit] = {
    val start = System.currentTimeMillis
    var firstOctetTimeMS = 0L
    val r = for {
      (fh, fb, columns) <- conn.stream("SELECT values FROM one LIMIT 1", One)
      // (fh, fb, columns) <- conn.stream("SELECT data FROM test LIMIT 1", One)
      // (fh, fb, columns) <- conn.stream("SELECT data FROM test LIMIT 300000", One)
      rows = columns.via(Flow.fromGraph(new Grouped[Column](fb.asInstanceOf[Result].header.asInstanceOf[Rows].columnsCount))).map(Row(_))
    } yield ResultSource(rows)

    r.flatMap { result =>
      // println("RESULT")
      result.rows.mapAsync(1) { row =>
        // println("ROW")
        row.columns.mapAsync(1) { column =>
          // println("\tCOLUMN")
          column.content.mapAsync(1) { bs =>
            // println("\t\tBYTES " + bs)
            if(firstOctetTimeMS == 0) firstOctetTimeMS = System.currentTimeMillis
            Future.successful(bs)
          }
          .runWith(Sink.seq)
        }
        .runWith(Sink.seq)
      }
      .runWith(Sink.seq)
    }.map(_ => ())
  }

  def get1(implicit conn: Connection, mat: Materializer): Future[Unit] = {
    for {
      (fh, fb, source) <- conn.stream1("SELECT data FROM test LIMIT 1", One)
      // (fh, fb, source) <- conn.stream1("SELECT values FROM one LIMIT 1", One)
      // (fh, fb, source) <- conn.stream1("SELECT data FROM test LIMIT 300000", One)
      // _ = println("Signet 1")
      bytes <- source.runWith(Sink.seq)
    } yield () //println("Signet 2")
  }
}
