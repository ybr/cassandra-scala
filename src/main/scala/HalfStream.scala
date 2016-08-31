package cassandra

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import akka.util.{ByteString, Timeout}

import cassandra.protocol._
import cassandra.streams._

import java.net.InetSocketAddress

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.util._

object HalfStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    implicit val materializer = ActorMaterializer.create(system)

    val conn = new Connection(new InetSocketAddress("192.168.99.100", 32769))
    val r: Future[Source[Column, NotUsed]] = for {
      keyspace <- conn.connect("proto")
      start = System.currentTimeMillis
      (fh, fb, columns) <- conn.stream("SELECT data FROM test LIMIT 300000", One)
    } yield {
      val end = System.currentTimeMillis
      println(s"------------------------- Duration = ${end - start} (ms)")
      columns
    }

    val r1 = r.flatMap { columns =>
      println("RESULT")
      columns.mapAsync(1) { column =>
        column.content.mapAsync(1) { bs =>
          // println("\t\tBYTES " + bs)
          Future.successful(bs)
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