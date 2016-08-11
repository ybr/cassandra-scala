package cassandra.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.stream._
import akka.stream.scaladsl._

import cassandra.decoder.FrameHeaderDecoder
import cassandra.protocol.FrameHeader

import org.scalatest._

import scala.concurrent._
import scala.concurrent.duration._

class StreamSplitterSpec extends FlatSpec with Matchers {
  it should "split stream" in {

    val bytes = ByteString(
      0x84, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x03, 0x00, 0x05, 0x70,
      0x72, 0x6f, 0x74, 0x6f
    )

    val source = Source(List(bytes, bytes))

    implicit val system = ActorSystem()

    implicit val materializer = ActorMaterializer.create(system)

    val flow = new StreamSplitter(FrameHeaderDecoder.frameHeader)

    val frameHeaders = Await.result(
      source
      .via(flow)
      .via(Flow[(FrameHeader, Source[ByteString, NotUsed])].map { case (header, bodySource) =>
        println("TOTOTOTOTOTO " + bodySource)
        val body = Await.result(bodySource.toMat(Sink.fold(List.empty[ByteString]) { (prev, curr) =>
          println("RESULT PREV = " + prev)
          println("RESULT CURR = " + curr)
          curr +: prev
        })(Keep.right).run(), 5 second)
        println("TOTOTOTOTOTO2 " + body)
        (header, body)
      })
      runWith(Sink.ignore),
      // .toMat(Sink.fold(List.empty[(FrameHeader, List[ByteString])])(_ :+ _))(Keep.right).run(),
      5 second
    )

    // val (header, body) = frameHeaders.head

    // println("HEADER = " + header)
    // println("BODY = " + body)

    system.shutdown()
    
    // header should === (FrameHeader(-124, 0x00, 0, cassandra.Opcode.Result, 11))
    // body.head should === (ByteString(0x00, 0x00, 0x00, 0x03, 0x00, 0x05, 112, 114, 111, 116, 111))
  }
}
