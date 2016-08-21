package cassandra.streams

import akka.NotUsed
import akka.stream.BidiShape
import akka.stream.scaladsl._
import akka.util.ByteString

import cassandra.protocol._
import cassandra.Encoders._
import cassandra.decoder._

object FrameHeaderBidi {
  val framing: BidiFlow[(FrameHeader, Source[ByteString, NotUsed]), ByteString, ByteString, (FrameHeader, Source[ByteString, NotUsed]), NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    val outbound = b.add(Flow[(FrameHeader, Source[ByteString, NotUsed])].flatMapConcat { case (fh, source) =>
      Source.single(fh.toBytes) concat source
    })

    val inbound = b.add(Flow.fromGraph(new StreamDetacher("FrameHeader", CassandraDecoders.frameHeader)))

    BidiShape.fromFlows(outbound, inbound)
  })
}