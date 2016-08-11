package cassandra.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.stream._
import akka.stream.scaladsl._

import cassandra.Opcode
import cassandra.decoder.FrameHeaderDecoder
import cassandra.protocol.FrameHeader

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

      val flow = new StreamDetacher(FrameHeaderDecoder.frameHeader)

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

      val flow = new StreamDetacher(FrameHeaderDecoder.frameHeader)

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

      val flow = new StreamDetacher(FrameHeaderDecoder.frameHeader)

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

      val flow = new StreamDetacher(FrameHeaderDecoder.frameHeader)

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
