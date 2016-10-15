package cassandra.streams

import akka.NotUsed
import akka.stream.BidiShape
import akka.stream.scaladsl._
import akka.util.ByteString

import cassandra.protocol._
import cassandra.Encoders._
import cassandra.decoder._

import scala.concurrent.duration._

object FrameHeaderBidi {
  val decoder = for {
    fh <- CassandraDecoders.frameHeader
    fb <- CassandraDecoders.frameBody(fh.opcode)
  } yield (fh, fb)

  def framing(implicit system: akka.actor.ActorSystem): BidiFlow[(FrameHeader, Source[ByteString, NotUsed]), ByteString, ByteString, DetachResult[(FrameHeader, FrameBody)], NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    val outbound = b.add(Flow[(FrameHeader, Source[ByteString, NotUsed])].flatMapConcat { case (fh, source) =>
        Source.single(fh.toBytes).concat(source)
        // try to send frame header and frame body with a same TCP packet
        .groupedWithin(2, 1 microseconds).map(_.foldLeft(ByteString.empty)(_ ++ _))
    })

    val inbound = b.add(Flow.fromGraph(new StreamDetacher(decoder)(_._1.length + 9)))

    BidiShape.fromFlows(outbound, inbound)
  })
}