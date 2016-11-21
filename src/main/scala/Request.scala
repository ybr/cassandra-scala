package cassandra

import akka.NotUsed
import akka.util.{ByteString, ByteStringBuilder}
import akka.stream.scaladsl.Source

import cassandra.protocol._

import com.github.ybr.cassandra._

object Request extends BodyEncoders with Encoders {
  val startup: (FrameHeader, Source[ByteString, NotUsed]) = {
    val body = Map("CQL_VERSION" -> "4.0.0").toBytes
    (FrameHeader(4, 0, 0, Opcode.Startup, body.length), Source.single(body))
  }

  val options: (FrameHeader, Source[ByteString, NotUsed]) = {
    (FrameHeader(4, 0, 0, Opcode.Options, 0), Source.empty)
  }

  implicit val uuidEncoder = Encoder[java.util.UUID] { uuid =>
    new ByteStringBuilder()
      .putInt(16)
      .putLong(uuid.getMostSignificantBits())
      .putLong(uuid.getLeastSignificantBits())
      .result()
  }

  def query(cql: String, cl: ConsistencyLevel, params: Seq[AnyRef]): (FrameHeader, Source[ByteString, NotUsed]) = {
    // Request[(LongString, ConsistencyLevel, Byte, Short)] = {
    val values: Byte = if(params.isEmpty) 0 else 1
    val byteParams: ByteString = params.map {
      case uuid: java.util.UUID => uuid.toBytes
    }.foldLeft(ByteString.empty)(_ ++ _)
    val paramsCount: Short = params.length.toShort
    val body = (new LongString(cql), cl, values).toBytes ++ paramsCount.toBytes ++ byteParams
    (FrameHeader(4, 0, 0, Opcode.Query, body.length), Source.single(body))
  }
}