package cassandra.decoder

import akka.util.ByteIterator

import java.nio.ByteOrder

import cassandra.BigEndian
import cassandra.protocol._

trait DefaultDecoders {
  def scalar[A](bytesSize: Int)(extract: ByteIterator => A)(implicit byteOrder: ByteOrder) = Decoder[A] { bs =>
    if(bs.length >= bytesSize) {
      val (payload, remaining) = bs.splitAt(bytesSize)
      Consumed(extract(payload.iterator), remaining)
    }
    else NotEnough
  }

  def double(implicit byteOrder: ByteOrder) = scalar[Double](8)(_.getDouble)
  def float(implicit byteOrder: ByteOrder) = scalar[Float](4)(_.getFloat)
  def long(implicit byteOrder: ByteOrder) = scalar[Long](8)(_.getLong)
  def int(implicit byteOrder: ByteOrder) = scalar[Int](4)(_.getInt)
  def short(implicit byteOrder: ByteOrder) = scalar[Short](2)(_.getShort)
  def byte(implicit byteOrder: ByteOrder) = scalar[Byte](1)(_.getByte)

  def list[A](size: Int)(A: Decoder[A]) = Decoder[List[A]] { bytes =>
    val decodedAS = (0 until size).foldLeft(DecoderResult.consumed(List.empty[A], bytes)) { (prev, _) =>
      prev match {
        case NotEnough => NotEnough
        case Consumed(as, bytes) =>
          A.decode(bytes) match {
            case Consumed(a, remaining) => Consumed(a :: as, remaining)
            case NotEnough => NotEnough
          }
      }
    }

    decodedAS match {
      case Consumed(as, remaining) => Consumed(as.reverse, remaining)
      case NotEnough => NotEnough
    }
  }

  def tuple[A, B](A: Decoder[A], B: Decoder[B]): Decoder[(A, B)] = for {
    a <- A
    b <- B
  } yield (a, b)

  def tuple[A, B, C](A: Decoder[A], B: Decoder[B], C: Decoder[C]): Decoder[(A, B, C)] = for {
    a <- A
    b <- B
    c <- C
  } yield (a, b, c)
}