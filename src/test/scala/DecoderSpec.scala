package cassandra.decoder

import akka.util.ByteString
import cassandra.decoder._

import org.scalatest._

class DecoderSpec extends FlatSpec with Matchers with DefaultDecoders with cassandra.BigEndian {
  it should "decode" in {
    val bytes = ByteString(0x01, 0x02)

    val Consumed(value, remaining) = byte.decode(bytes)
    value shouldBe (1)
    remaining shouldBe (ByteString(0x02))
  }
}