package cassandra

import akka.NotUsed
import akka.util.{ByteString, ByteStringBuilder}
import akka.stream.scaladsl.Source

import cassandra.protocol._

object Request extends BodyEncoders with Encoders {
  val startup: (FrameHeader, Source[ByteString, NotUsed]) = {
    val body = Map("CQL_VERSION" -> "4.0.0").toBytes
    (FrameHeader(4, 0, 0, Opcode.Startup, body.length), Source.single(body))
  }

  val options: (FrameHeader, Source[ByteString, NotUsed]) = {
    (FrameHeader(4, 0, 0, Opcode.Options, 0), Source.empty)
  }

  def query(cql: String, cl: ConsistencyLevel): (FrameHeader, Source[ByteString, NotUsed]) = {
    // Request[(LongString, ConsistencyLevel, Byte, Short)] = {
    val body = (new LongString(cql), cl, 0x01: Byte, 0x0000).toBytes
    (FrameHeader(4, 0, 0, Opcode.Query, body.length), Source.single(body))
  }
}