package cassandra.protocol

import akka.NotUsed
import akka.stream.BidiShape
import akka.stream.scaladsl._
import akka.util.ByteString

import cassandra.protocol.FrameHeader._
import cassandra.Encoders._

import cassandra._

import streams.StreamDetacher
import decoder.FrameHeaderDecoder

import util.ByteStrings

object FrameHeaderBidi {
  val framing: BidiFlow[Frame, ByteString, ByteString, Frame, NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    val outbound = b.add(Flow[Frame].flatMapConcat { frame =>
      Source.single(frame.header.toBytes) concat frame.body
    })

    val inbound = b.add(Flow.fromGraph(new StreamDetacher(FrameHeaderDecoder.frameHeader)).map(Frame.apply _ tupled))

    BidiShape.fromFlows(outbound, inbound)
  })

  val dump: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    def ios(direction: String) = Flow[ByteString].map { bytes =>
      println(direction)
      println(ByteStrings.dump(bytes))
      bytes
    }

    BidiShape.fromFlows(b.add(ios("I ~> O")), b.add(ios("I <~ O")))
  })
}