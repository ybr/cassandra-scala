package cassandra

import akka.util.{ByteString, ByteStringBuilder}
import akka.stream.scaladsl.Source

import cassandra.protocol._

object Request extends BodyEncoders with Encoders {
  val startup: Frame = {
    val body = Map("CQL_VERSION" -> "4.0.0").toBytes
    Frame(FrameHeader(4, 0, 0, Opcode.Startup, body.length), Source.single(body))
  }

  val options: Frame = {
    val body = NoBody.toBytes
    Frame(FrameHeader(4, 0, 0, Opcode.Options, body.length), Source.single(body))
  }

  def query(cql: String, cl: ConsistencyLevel): Frame = {
    // Request[(LongString, ConsistencyLevel, Byte, Short)] = {
    val body = (new LongString(cql), cl, 0x01, 0x0000).toBytes
    Frame(FrameHeader(4, 0, 0, Opcode.Query, body.length), Source.single(body))
  }
}