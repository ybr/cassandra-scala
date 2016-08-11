package cassandra.protocol

import akka.util.{ByteString, ByteStringBuilder}

import cassandra._
import cassandra.Encoders._

case class FrameHeader(
  version: Byte,
  flags: Byte,
  stream: Short,
  opcode: Opcode,
  length: Int) {

  override def toString(): String = f"FrameHeader(0x$version%02X, 0x$flags%02X, $stream, $opcode, $length)"
}

object FrameHeader extends BigEndian {
  implicit val frameHeaderEncoder = Encoder[FrameHeader] { fh =>
    new ByteStringBuilder()
      .putByte(fh.version)
      .putByte(fh.flags)
      .putShort(fh.stream)
      .append(fh.opcode.toBytes)
      .putInt(fh.length)
      .result()
  }
}