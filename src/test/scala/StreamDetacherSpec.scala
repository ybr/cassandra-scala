package cassandra.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.stream._
import akka.stream.scaladsl._

import cassandra.decoder._
import cassandra.protocol._

import org.scalatest._

import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class StreamDetacherSpec extends FlatSpec with Matchers {
  val headerBodyFlow = Flow[DetachResult[FrameHeader]].flatMapConcat {
    case Complete(header, bytes) => Source.single(header -> bytes)
    case Partial(header, source) => source.via(Flow[ByteString].map(b => (header, b)))
  }

  it should "push one element" in {
    WithMaterializer { implicit materializer =>
      val bytes = ByteString(0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0b)

      val flow = new StreamDetacher(CassandraDecoders.frameHeader)(_ => 9)

      val frameHeaders = Await.result(
        Source.single(bytes)
        .via(flow)
        .runWith(Sink.seq),
        1 second
      )

      frameHeaders.head should === (Complete(FrameHeader(-124, 0x00, 0, Opcode.Result, 11), ByteString.empty))
    }
  }

  it should "push many elements" in {
    val count = 10
    WithMaterializer { implicit materializer =>
      val bytes = ByteString(0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b)

      val flow = new StreamDetacher(CassandraDecoders.frameHeader)(_.length + 9)

      val frameHeaders = Await.result(
        Source.repeat(bytes).take(count)
        .via(flow)
        .via(headerBodyFlow)
        .runWith(Sink.seq),
        1 second
      )

      frameHeaders.size should === (count)
    }
  }

  it should "push element once enough data is available for decoder" in {
    WithMaterializer { implicit materializer =>
      val bytes = immutable.Iterable(
        ByteString(0x84, 0x00, 0x00), // head chunk
        ByteString(0x00), // head chunk
        ByteString(0x08, 0x00, 0x00, 0x00, 0x0b), // head chunk
        ByteString(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07), // body chunk
        ByteString(0x08, 0x09, 0x0a, 0x0b, 0x0c) // body chunk
      )

      val flow = new StreamDetacher(CassandraDecoders.frameHeader)(_.length + 9)

      val frameHeaders = Await.result(
        Source(bytes)
        .via(flow)
        .via(headerBodyFlow)
        .runWith(Sink.seq),
        1 second
      )

      val (headers, bodies) = frameHeaders.unzip

      headers(0) should === (FrameHeader(-124, 0x00, 0, Opcode.Result, 11))
      headers(1) should === (FrameHeader(-124, 0x00, 0, Opcode.Result, 11))
      bodies(0) should === (ByteString(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07))
      bodies(1) should === (ByteString(0x08, 0x09, 0x0a, 0x0b))
    }
  }

  it should "push element and handle more bytes" in {
    WithMaterializer { implicit materializer =>
      val bytes = ByteString(0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c)

      val flow = new StreamDetacher(CassandraDecoders.frameHeader)(_.length + 9)

      val frameHeaders = Await.result(
        Source.single(bytes)
        .via(flow)
        .via(headerBodyFlow)
        .runWith(Sink.seq),
        1 second
      )

      val (frameHeader, body) = frameHeaders.head

      frameHeader should === (FrameHeader(-124, 0x00, 0, Opcode.Result, 11))
      body should === (ByteString(0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b))
    }
  }

  it should "detach on two levels" in {
    WithMaterializer { implicit materializer =>
      val bytes = immutable.Iterable(
        ByteString(0x00, 0x00, 0x00, 0x01, 0x00, 0x00),
        ByteString(0x00, 0x02, 0x00),
        ByteString(0x00, 0x00, 0x03, 0x00, 0X00, 0x00, 0x04)
      )

      import CassandraDecoders._

      val flow = new StreamDetacher(int(java.nio.ByteOrder.BIG_ENDIAN))(_ => 8)

       val result = Await.result(Source(bytes)
        .via(flow)
        .via(Flow[DetachResult[Int]].mapAsync(1) {
          case Complete(level1, bytes) =>
            println("Complete level 1 " + level1)
            val Consumed(level2, _) = int(java.nio.ByteOrder.BIG_ENDIAN).decode(bytes)
            println("Level 2 " + level2)
            Future.successful(level1 -> level2)
          case Partial(level1, source) =>
            println("Partial level1 " + level1)
            source
              .via(new StreamDetacher(int(java.nio.ByteOrder.BIG_ENDIAN))(_ => 4))
              .via(Flow[DetachResult[Int]].mapAsync(1) {
                case Complete(level2, _) =>
                  println("Complete level 2 " + level2)
                  Future.successful(level1 -> level2)
                case Partial(level2, source) =>
                  println("Partial level 2 " + level2)
                  source.runWith(Sink.ignore).map { _ =>
                    (level1 -> level2)
                  }
              })
              .runWith(Sink.seq)
        })
        .runWith(Sink.seq),
        2 second)

      println("Coucou " + result)
    }
  }

  ignore should "detach on two levels with second level no consumption (empty body)" in {

  }
}

object WithMaterializer {
  def apply[A](f: Materializer => A): A = {
    val system = ActorSystem()
    val materializer = ActorMaterializer.create(system)
    val a = f(materializer)
    system.shutdown()
    a
  }
}
