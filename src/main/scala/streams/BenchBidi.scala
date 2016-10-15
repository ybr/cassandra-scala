package cassandra.streams

import akka.NotUsed
import akka.stream.BidiShape
import akka.stream.scaladsl._
import akka.util.ByteString

import util.ByteStrings

object BenchBidi {
  val dump: BidiFlow[ByteString, ByteString, ByteString, ByteString, NotUsed] = BidiFlow.fromGraph(GraphDSL.create() { implicit b =>
    var start = System.currentTimeMillis

    BidiShape.fromFlows(
      b.add(Flow[ByteString].map { bytes =>
        start = System.currentTimeMillis
        bytes
      }),
      b.add(Flow[ByteString].map { bytes =>
        val end = System.currentTimeMillis
        println(s"Message took ${end-start}(ms)")
        bytes
      })
    )
  })
}