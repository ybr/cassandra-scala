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
  val headerBodyFlow = Flow[(FrameHeader, Source[ByteString, NotUsed])].flatMapConcat { case (header, body) =>
    body.via(Flow[ByteString].map(b => (header, b)))
  }

  it should "push one element" in {
    WithMaterializer { implicit materializer =>
      val bytes = ByteString(0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0b)

      val flow = new StreamDetacher("FrameHeader", CassandraDecoders.frameHeader)

      val frameHeaders = Await.result(
        Source.single(bytes)
        .via(flow)
        .runWith(Sink.seq),
        1 second
      )

      frameHeaders.head._1 should === (FrameHeader(-124, 0x00, 0, Opcode.Result, 11))
    }
  }

  it should "push many elements" in {
    val count = 10
    WithMaterializer { implicit materializer =>
      val bytes = ByteString(0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0b, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b)

      val flow = new StreamDetacher("FrameHeader", CassandraDecoders.frameHeader)

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

      val flow = new StreamDetacher("FrameHeader", CassandraDecoders.frameHeader)

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

      val flow = new StreamDetacher("FrameHeader", CassandraDecoders.frameHeader)

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
        ByteString(0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x04),
        ByteString(0x05, 0x06)
      )

      import CassandraDecoders._

      val flow = new StreamDetacher("int0", int(java.nio.ByteOrder.BIG_ENDIAN).more(_ => 7))

       val result = Await.result(Source(bytes)
        .via(flow)
        .via(Flow[(Int, Source[ByteString, NotUsed])].mapAsync(1) { case (b, source) =>
          println("IN level 1 " + b)
          source
            .via(new StreamDetacher("int1", int(java.nio.ByteOrder.BIG_ENDIAN).more(_ => 3)))
            .via(Flow[(Int, Source[ByteString, NotUsed])].mapAsync(1) { case (b2, source2) =>
              println("IN level 2 " + b2)
              source2.runWith(Sink.seq)
              // source2.runWith(Sink.foreach { bs2 =>
              //   println("IN level 2 bs " + bs2)
              // })
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
