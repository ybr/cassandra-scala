package cassandra.streams

import akka.NotUsed
import akka.stream.{BidiShape, Materializer}
import akka.stream.scaladsl._
import akka.util.ByteString

import cassandra.protocol._
import cassandra.Encoders._
import cassandra.decoder._

object FrameBodyBidi {
  def framing(implicit mat: Materializer, system: akka.actor.ActorSystem): BidiFlow[
    (FrameHeader, Source[ByteString, NotUsed]),
    (FrameHeader, Source[ByteString, NotUsed]),
    (FrameHeader, Source[ByteString, NotUsed]),
    (FrameHeader, FrameBody, Source[ByteString, NotUsed]),
    NotUsed
  ] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    val outbound = b.add(Flow[(FrameHeader, Source[ByteString, NotUsed])].map(identity))

    val inbound = b.add(Flow[(FrameHeader, Source[ByteString, NotUsed])].flatMapConcat { case (fh, source) =>
      source
      .via(Flow.fromGraph(new StreamDetacher(Decoder.more(fh.length).flatMap(_ => CassandraDecoders.frameBody(fh.opcode)))).map { case (fb, source) =>
        (fh, fb, source) // here is it the correct source ??
      })
    })

    BidiShape.fromFlows(outbound, inbound)
  })
}