package cassandra.decoder

import akka.util.ByteString
import cassandra._

trait CassandraTypesDecoders extends DefaultDecoders with BigEndian {
  val bytes: Decoder[ByteString] = for {
    size <- int
    bytes <- Decoder { bytes =>
      if(bytes.length >= size) {
        val (payload, remaining) = bytes.splitAt(size)
        Consumed(payload, remaining)
      }
      else NotEnough
    }
  } yield bytes

  val string: Decoder[String] = for {
    size <- short
    bytes <- scalar[Array[Byte]](size)(_.getBytes(size))
  } yield new String(bytes, "UTF-8")

  val strings: Decoder[List[String]] = for {
    size <- short
    strings <- list(size)(string)
  } yield strings

  val multimap: Decoder[Map[String, List[String]]] = for {
    size <- short
    multilist <- list(size)(tuple(string, strings))
  } yield multilist.toMap
}

object CassandraTypesDecoders extends CassandraTypesDecoders