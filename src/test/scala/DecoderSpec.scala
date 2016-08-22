package cassandra.decoder

import akka.util.ByteString
import cassandra.decoder._

import org.scalatest._

class DecoderSpec extends FlatSpec with Matchers with DefaultDecoders with cassandra.BigEndian {
  it should "decrease more bytes required" in {
    val bytes = ByteString(0x01, 0x02)

    val Consumed(value, remaining, more) = Decoder.more(5).flatMap(_ => byte).decode(bytes)
    value shouldBe (1)
    remaining shouldBe (ByteString(0x02))
    more shouldBe (4)
  }
}