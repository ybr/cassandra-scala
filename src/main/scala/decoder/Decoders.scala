package cassandra.decoder

import akka.util.ByteIterator

import java.nio.ByteOrder

import cassandra.{BigEndian, Opcode}
import cassandra.protocol.FrameHeader

trait SizeOf[A] {
  def inBytes(): Int
}

object SizeOf {
  def apply[A](size: Int) = new SizeOf[A] {
    val inBytes = size
  }
}

trait SizeOfs {
  implicit val sizeOfDouble = SizeOf[Double](8)
  implicit val sizeOfFloat = SizeOf[Float](4)
  implicit val sizeOfLong = SizeOf[Long](8)
  implicit val sizeOfInt = SizeOf[Int](4)
  implicit val sizeOfShort = SizeOf[Short](2)
  implicit val sizeOfByte = SizeOf[Byte](1)
}

object SizeOfs extends SizeOfs

import cassandra.decoder.SizeOfs._

trait Decoders {
  def scalar[A](extract: ByteIterator => A)(implicit byteOrder: ByteOrder, size: SizeOf[A]) = Decoder[A] { bs =>
    if(bs.length >= size.inBytes) {
      val (payload, remaining) = bs.splitAt(size.inBytes)
      Consumed(extract(payload.iterator), remaining, 0)
    }
    else NotEnough
  }

  def double(implicit byteOrder: ByteOrder) = scalar[Double](_.getDouble)
  def float(implicit byteOrder: ByteOrder) = scalar[Float](_.getFloat)
  def long(implicit byteOrder: ByteOrder) = scalar[Long](_.getLong)
  def int(implicit byteOrder: ByteOrder) = scalar[Int](_.getInt)
  def short(implicit byteOrder: ByteOrder) = scalar[Short](_.getShort)
  def byte(implicit byteOrder: ByteOrder) = scalar[Byte](_.getByte)
}

object FrameHeaderDecoder extends BigEndian {
  val opcode: Decoder[Opcode] = byte.map(Opcode.fromByte)

  val frameHeader = Decoder[FrameHeader] { bytes =>
    if(bytes.length >= 9) {
      val (payload, remaining) = bytes.splitAt(9)
      val reader = payload.iterator
      val version = reader.getByte
      val flags = reader.getByte
      val stream = reader.getShort
      val opcode = Opcode.fromByte(reader.getByte)
      val length = reader.getInt
      Consumed(
        FrameHeader(version, flags, stream, opcode, length),
        remaining,
        length
      )
    }
    else NotEnough
  }
}