package cassandra

import cassandra.protocol._

import com.github.ybr.cassandra._

import akka.util.{ByteString, ByteStringBuilder}
import java.nio.ByteOrder

trait BigEndian {
  implicit val bigEndiantByteOrder: ByteOrder = ByteOrder.BIG_ENDIAN
}

trait Encoders extends EncoderOps with BigEndian {
  implicit def tuple3Encoder[A: Encoder, B: Encoder, C: Encoder] = Encoder[(A, B, C)] { t3 =>
    new ByteStringBuilder()
      .append(t3._1.toBytes)
      .append(t3._2.toBytes)
      .append(t3._3.toBytes)
      .result()
  }

  implicit def tuple4Encoder[A: Encoder, B: Encoder, C: Encoder, D: Encoder] = Encoder[(A, B, C, D)] { t =>
    new ByteStringBuilder()
      .append(t._1.toBytes)
      .append(t._2.toBytes)
      .append(t._3.toBytes)
      .append(t._4.toBytes)
      .result()
  }

  implicit val opcodeEncoder = Encoder[Opcode] { op =>
    ByteString(op match {
      case Opcode.Error => 0x00
      case Opcode.Startup => 0x01
      case Opcode.Ready => 0x02
      case Opcode.Authenticate => 0x03
      case Opcode.Options => 0x05
      case Opcode.Supported => 0x06
      case Opcode.Query => 0x07
      case Opcode.Result => 0x08
      case Opcode.Prepare => 0x09
      case Opcode.Execute => 0x0A
      case Opcode.Register => 0x0B
      case Opcode.Event => 0x0C
      case Opcode.Batch => 0x0D
      case Opcode.AuthChallenge => 0x0E
      case Opcode.AuthResponse => 0x0F
      case Opcode.AuthSuccess => 0x10
    })
  }

  implicit val consistencyLevelEncoder = Encoder[ConsistencyLevel] { cl =>
    val short: Short = cl match {
      case Any => 0x0000
      case One => 0x0001
      case Two => 0x0002
      case Three => 0x0003
      case Quorum => 0x0004
      case All => 0x0005
      case LocalQuorum => 0x0006
      case EachQuorum => 0x0007
      case Serial => 0x0008
      case LocalSerial => 0x0009
      case LocalOne => 0x000A
    }
    new ByteStringBuilder().putShort(short).result()
  }
}

trait BodyEncoders extends EncoderOps with BigEndian {
  implicit val byteEncoder = Encoder[Byte](byte => ByteString(byte))
  implicit val shortEncoder = Encoder[Short](short => new ByteStringBuilder().putShort(short).result())
  implicit val intEncoder = Encoder[Int](int => new ByteStringBuilder().putInt(int).result())
  implicit val longEncoder = Encoder[Long](long => new ByteStringBuilder().putLong(long).result())

  implicit val bytesEncoder = Encoder[List[Byte]] { bytes =>
    new ByteStringBuilder()
      .putInt(bytes.length)
      .putBytes(bytes.toArray)
      .result()
  }

  implicit val stringEncoder = Encoder[String] { str =>
    new ByteStringBuilder()
      .putShort(str.length)
      .putBytes(str.getBytes())
      .result()
  }

  implicit val longStringEncoder = Encoder[LongString] { ls =>
    new ByteStringBuilder()
      .putInt(ls.str.length)
      .putBytes(ls.str.getBytes())
      .result()
  }

  implicit val stringListEncoder = Encoder[List[String]] { strings =>
    strings.foldLeft(new ByteStringBuilder().putShort(strings.length)) { (builder, str) =>
      builder.append(str.toBytes)
    }
    .result()
  }

  implicit val stringMapEncoder = Encoder[Map[String, String]] { strings =>
    strings.foldLeft(new ByteStringBuilder().putShort(strings.size)) { (builder, entry) =>
      builder
        .append(entry._1.toBytes) // key
        .append(entry._2.toBytes) // value
    }
    .result()
  }
}

object Encoders extends Encoders with BodyEncoders