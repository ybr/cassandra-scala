package cassandra

import akka.util.ByteString

trait Encoder[T] {
  def toBytes(t: T): ByteString
}

trait EncoderOps {
  implicit class EncoderWrapper[T](t: T) extends AnyRef {
    def toBytes(implicit encoder: Encoder[T]): ByteString = encoder.toBytes(t)
  }
}

object Encoder {
  def apply[T](encoder: T => ByteString) = new Encoder[T]{
    def toBytes(t: T): ByteString = encoder(t)
  }

  def toBytes[T](t: T)(implicit encoder: Encoder[T]): ByteString = encoder.toBytes(t)
}