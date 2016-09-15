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
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
      _ <- Dao.get1(conn, materializer)
    } yield ()

    Try(Await.result(r1.recoverWith {
      case t =>
        println("ERROR TOTO " + t.getMessage)
        t.printStackTrace
        Future(())
    }, 50 second)) match {
      case Success(v) => println("OK")
      case Failure(t) => t.printStackTrace
    }

    system.terminate()
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

  val detacher  = Flow.fromGraph(new StreamDetacher(CassandraDecoders.int.more(identity)))
                  .map(t => Column(t._2))

  def stream(query: String, cl: ConsistencyLevel): Future[(FrameHeader, FrameBody, Source[Column, NotUsed])] = for {
    (fh, fb, source) <- actorRef ? Request.query(query, cl) map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
    rowsHeader = fb.asInstanceOf[Result].header.asInstanceOf[Rows]
    result = source.via(detacher)
  } yield (fh, fb, result)

  def stream1(query: String, cl: ConsistencyLevel): Future[(FrameHeader, FrameBody, Source[ByteString, NotUsed])] = {
    actorRef ? Request.query(query, cl) map(_.asInstanceOf[(FrameHeader, FrameBody, Source[ByteString, NotUsed])])
  }
}

class ConnectionActor(remote: InetSocketAddress)(implicit system: ActorSystem) extends Actor {
  implicit val materializer = ActorMaterializer.create(system)

  var listenerRef: Option[ActorRef] = None

  val notifyListener = Flow[(FrameHeader, FrameBody, Source[ByteString, akka.NotUsed])].map { f =>
    self ! f
    f
  }

  val runnable = Source.actorRef(1, OverflowStrategy.fail) // here might be here why it fails
                  .via(
                    Fusing.aggressive(
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
                  ))
                  .via(notifyListener)
                  .to(Sink.ignore)
  val tcpActor: ActorRef = runnable.run()

  def receive = {
    case frame @ (FrameHeader(version, _, _, _, _), source) if version == 0x04 => // request
      listenerRef = Some(sender)
      tcpActor ! frame

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

object Dao {
  def get(implicit conn: Connection, mat: Materializer): Future[Unit] = {
    val start = System.currentTimeMillis
    var firstOctetTimeMS = 0L
    val r = for {
      // (fh, fb, columns) <- conn.stream("SELECT values FROM one LIMIT 10", One)
      (fh, fb, columns) <- conn.stream("SELECT data FROM test LIMIT 1", One)
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
    }.map { bytes =>
      val end = System.currentTimeMillis
      println(bytes.asInstanceOf[Vector[Vector[Vector[ByteString]]]](0)(0).map(_.size).sum)
      println("ROWS " + bytes.asInstanceOf[Vector[Vector[Vector[ByteString]]]].size)
      println(s"First byte duration: ${firstOctetTimeMS - start}(ms)")
      println(s"Total duration: ${end - start}(ms)")
    }
  }

  def get1(implicit conn: Connection, mat: Materializer): Future[Unit] = {
    val start = System.currentTimeMillis
    var firstOctetTimeMS = 0L
    val r = for {
      (fh, fb, source) <- conn.stream1("SELECT data FROM test LIMIT 1", One)
      // (fh, fb, source) <- conn.stream1("SELECT data FROM test LIMIT 300000", One)
      bytes <- source.mapAsync(1) { bs =>
        if(firstOctetTimeMS == 0) firstOctetTimeMS = System.currentTimeMillis
        Future.successful(bs)
      }
      .runWith(Sink.seq)
    } yield bytes


    r.map { bytes =>
      val end = System.currentTimeMillis
      println("Bytes " + bytes.asInstanceOf[Vector[ByteString]].map(_.size).sum)
      println(s"First byte duration: ${firstOctetTimeMS - start}(ms)")
      println(s"Total duration: ${end - start}(ms)")
    }
  }
}