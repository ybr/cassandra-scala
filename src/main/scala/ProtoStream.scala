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

import com.github.ybr.cassandra._

import java.util.UUID

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._
import scala.util._

object ProtoStream {
  def main(args: Array[String]) {
    implicit val system = ActorSystem()

    implicit val materializer = ActorMaterializer.create(system)

    val cluster = Cluster("192.168.99.100:32769")
    implicit val session: Session = Await.result(cluster.connect("proto"), 1 second)

    val start = System.currentTimeMillis
    val r1: Future[Unit] = for {
      _ <- loop(1)(Dao.get1(session, materializer))
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

object Dao {
  def get()(implicit session: Session): Future[Unit] = {
    for {
      rs <- session.execute(One)("SELECT * FROM test LIMIT 1")
    } yield println(rs.columnDefinitions())
  }

  def get1(implicit session: Session, mat: Materializer): Future[Unit] = {
    for {
      // resultSource <- session.execute(One)("SELECT * FROM test LIMIT 1")
      // resultSource <- session.execute(One)("SELECT * FROM test LIMIT 300000")
      resultSource <- session.execute(One)(
        "SELECT * FROM test WHERE job_id = ? LIMIT 10",
        UUID.fromString("42b5a396-9c28-4f84-85b4-e49a088ed29e")
      )
      // 42b5a396-9c28-4f84-85b4-e49a088ed29e
      // a3ffefd1-609b-48ec-a646-c28626744554
      // _ = println("Signet 1")
      // start = System.currentTimeMillis()
      // bs <- resultSource.columns
      //         .flatMapConcat(_.content)
      //         .runFold(ByteString.empty) { (a1, a2) =>
      //           val end = System.currentTimeMillis()
      //           println(s"Bytes received after ${end-start}(ms)")
      //           a1 ++ a2
      //         }
      uuids <- resultSource.all().map { row =>
        // row.map { r =>
        //   val bytes = r.blob("values").get
        //   println(bytes.length)
        // }
        row.map(_.uuid("job_id").get)
      }
    } yield {
      if(resultSource.ok())
        println(uuids.toList)
        // println("END " + bs.length)
      else println(resultSource.errorMessage())
    }
  }
}
