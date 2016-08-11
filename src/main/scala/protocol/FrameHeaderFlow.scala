package cassandra.protocol

import akka.NotUsed
import akka.stream.BidiShape
import akka.stream.scaladsl._
import akka.util.ByteString

import cassandra.protocol.FrameHeader._
import cassandra.Encoders._
import cassandra.Decoders._

import cassandra._

object FrameHeaderBidi {
  val framing: BidiFlow[Frame, ByteString, ByteString, Frame, NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    val outbound = b.add(Flow[Frame].flatMapConcat { frame =>
      Source.single(frame.header.toBytes) concat frame.body
    })

    val inbound = b.add(Flow.fromGraph(new FrameParserStage()))

    // val inbound = b.add(Flow[ByteString].map { bytes =>
    //   val (fh, remaining) = bytes.fromBytes[FrameHeader]
    //   new Frame(fh, Source.single(remaining))
    // })

    BidiShape.fromFlows(outbound, inbound)
  })

  val dump: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    import cassandra.ByteStrings

    def ios(direction: String) = Flow[ByteString].map { bytes =>
      println(direction)
      println(ByteStrings.dump(bytes))
      bytes
    }

    BidiShape.fromFlows(b.add(ios("I ~> O")), b.add(ios("I <~ O")))
  })
}