package cassandra

import akka.util.ByteString

//             needed = stash.iterator.getInt

trait Decoder[T] {
  def fromBytes(bytes: ByteString): (T, ByteString)
}

trait DecoderOps {
  implicit class DecoderWrapper(bytes: ByteString) extends AnyRef {
    def fromBytes[T](implicit decoder: Decoder[T]): (T, ByteString) = decoder.fromBytes(bytes)
  }
}

object Decoder {
  def apply[T](decoder: ByteString => (T, ByteString)) = new Decoder[T]{
    def fromBytes(bytes: ByteString): (T, ByteString) = decoder(bytes)
  }

  def fromBytes[T](bytes: ByteString)(implicit decoder: Decoder[T]): (T, ByteString) = decoder.fromBytes(bytes)
}