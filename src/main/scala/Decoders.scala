package cassandra

import akka.util.ByteString

trait Decoders extends DecoderOps {
  def list[T: Decoder](n: Int) = Decoder[List[T]] { bytes =>
    var currentPayload = bytes

    var ts = Vector.empty[T]
    while(ts.length < n) {
      val (t, newPayload) = currentPayload.fromBytes[T]
      ts = ts :+ t
      currentPayload = newPayload
    }

    (ts.toList, currentPayload)
  }

  implicit def tuple2Decoder[A: Decoder, B: Decoder] = Decoder[(A, B)] { bytes =>
    val (a, remainingA) = bytes.fromBytes[A]
    val (b, remainingB) = remainingA.fromBytes[B]
    (a -> b, remainingB)
  }

  implicit def tuple3Decoder[A: Decoder, B: Decoder, C: Decoder] = Decoder[(A, B, C)] { bytes =>
    val (a, remainingA) = bytes.fromBytes[A]
    val (b, remainingB) = remainingA.fromBytes[B]
    val (c, remainingC) = remainingB.fromBytes[C]
    ((a, b, c), remainingC)
  }

  implicit val byteDecoder = Decoder[Byte] { bytes =>
    val (payload, remaining) = bytes.splitAt(1)
    (payload.asByteBuffer.get(), remaining)
  }

  implicit val intDecoder = Decoder[Int] { bytes =>
    val (payload, remaining) = bytes.splitAt(4)
    (payload.asByteBuffer.getInt(), remaining)
  }

  implicit val shortDecoder = Decoder[Short] { bytes =>
    val (payload, remaining) = bytes.splitAt(2)
    (payload.asByteBuffer.getShort(), remaining)
  }

  implicit val stringDecoder = Decoder[String] { bytes =>
    val size = bytes.toByteBuffer.getShort
    val (payload, remaining) = bytes.drop(2).splitAt(size)
    (new String(payload.toArray, "UTF-8"), remaining)
  }

  implicit val byteListDecoder = Decoder[List[Byte]] { bytes =>
    val n = bytes.toByteBuffer.getInt
    if(n >= bytes.length) println("????????????????????????? N overflows the bytestring")
    var (bs, remaining) = bytes.drop(4).splitAt(n)
    (bs.toList, remaining)
  }

  implicit val stringListDecoder = Decoder[List[String]] { bytes =>
    val size = bytes.toByteBuffer.getShort
    var currentPayload = bytes.drop(2)
    var strings = Vector.empty[String]
    while(strings.length < size) {
      val (str, newPayload) = currentPayload.fromBytes[String]
      strings = strings :+ str
      currentPayload = newPayload
    }
    (strings.toList, currentPayload)
  }

  implicit val stringMultiMap = Decoder[Map[String, List[String]]] { bytes =>
    val n = bytes.toByteBuffer.getShort()
    var currentPayload = bytes.drop(2)
    var multiMap = Map.empty[String, List[String]]
    while(multiMap.size < n) {
      val (key, payloadAfterKey) = currentPayload.fromBytes[String]
      val (values, payloadAfterValues) = payloadAfterKey.fromBytes[List[String]]
      multiMap = multiMap + (key -> values)
      currentPayload = payloadAfterValues
    }
    (multiMap, currentPayload)
  }

  implicit val opcodeDecoder = Decoder[Opcode] { bytes =>
    val (opcodeBytes, remaining) = bytes.splitAt(1)
    val opcode = opcodeBytes.head match {
      case 0x00 => Opcode.Error
      case 0x01 => Opcode.Startup
      case 0x02 => Opcode.Ready
      case 0x03 => Opcode.Authenticate
      case 0x05 => Opcode.Options
      case 0x06 => Opcode.Supported
      case 0x07 => Opcode.Query
      case 0x08 => Opcode.Result
      case 0x09 => Opcode.Prepare
      case 0x0A => Opcode.Execute
      case 0x0B => Opcode.Register
      case 0x0C => Opcode.Event
      case 0x0D => Opcode.Batch
      case 0x0E => Opcode.AuthChallenge
      case 0x0F => Opcode.AuthResponse
      case 0x10 => Opcode.AuthSuccess
    }
    (opcode, remaining)
  }

  implicit val columnTypeDecoder = Decoder[ColumnType] { bytes =>
    val (columnTypeShort, remaining) = bytes.fromBytes[Short]
    val columnType = columnTypeShort match {
      case 0x0000 => ColumnType.Custom
      case 0x0001 => ColumnType.Ascii
      case 0x0002 => ColumnType.Bigint
      case 0x0003 => ColumnType.Blob
      case 0x0004 => ColumnType.Boolean
      case 0x0005 => ColumnType.Counter
      case 0x0006 => ColumnType.Decimal
      case 0x0007 => ColumnType.Double
      case 0x0008 => ColumnType.Float
      case 0x0009 => ColumnType.Int
      case 0x000B => ColumnType.Timestamp
      case 0x000C => ColumnType.Uuid
      case 0x000D => ColumnType.Varchar
      case 0x000E => ColumnType.Varint
      case 0x000F => ColumnType.Timeuuid
      case 0x0010 => ColumnType.Inet
      case 0x0020 => ColumnType.List
      case 0x0021 => ColumnType.Map
      case 0x0022 => ColumnType.Set
      case 0x0030 => ColumnType.Udt
      case 0x0031 => ColumnType.Tuple
    }
    (columnType, remaining)
  }

  implicit val setKeyspaceDecoder = Decoder[SetKeyspace] { bytes =>
    val (keyspace, remaining) = bytes.fromBytes[String]
    (SetKeyspace(keyspace), remaining)
  }

  implicit val rowsResultHeaderDecoder = Decoder[Rows] { bytes =>
    val (flags, flagsRemaining) = bytes.fromBytes[Int]
    val (columnsCount, columnsCountRemaining) = flagsRemaining.fromBytes[Int]

    val (pagingState, pagingStateRemaining) = if(flags.&(0x0002: Int) == 2) {
        val (pagingState, pagingStateRemaining) = columnsCountRemaining.fromBytes[List[Byte]]
        (Some(pagingState), pagingStateRemaining)
      }
      else (None, columnsCountRemaining)

    val (globalTableSpec, globalTableSpecRemaining) = if(flags.&(0x0001: Int) == 1) {
      val (globalTableSpec, globalTableSpecRemaining) = columnsCountRemaining.fromBytes[(String, String)]
      (Some(globalTableSpec), globalTableSpecRemaining)
    }
    else (None, columnsCountRemaining)

    val (colSpecs, colSpecsRemaining) = if(flags.&(0X0001: Int) == 0) {
      val (colSpecs, colSpecsRemaining) = globalTableSpecRemaining.fromBytes(list[((String, String), String, ColumnType)](columnsCount))
      (colSpecs.map { case (keyspaceTable, name, colType) => (Some(keyspaceTable), name, colType)}, colSpecsRemaining)
    }
    else {
      val (colSpecs, colSpecsRemaining) = globalTableSpecRemaining.fromBytes(list[(String, ColumnType)](columnsCount))
      (colSpecs.map { case (name, colType) => (None, name, colType)}, colSpecsRemaining)
    }

    val (rowsCount, rowsCountRemaining) = colSpecsRemaining.fromBytes[Int]
    (Rows(flags, columnsCount, pagingState, globalTableSpec, colSpecs, rowsCount), rowsCountRemaining)
  }

  implicit val resultHeaderDecoder = Decoder[ResultHeader] { bytes =>
    val (kind, remaining) = bytes.fromBytes[Int]
    val (resultHeader, resultBodyRemaining) = kind match {
      case 1 => (Void, ByteString.empty)
      case 2 => remaining.fromBytes[Rows]
      case 3 => remaining.fromBytes[SetKeyspace]
      case 4 => (Prepared, ByteString.empty)
      case 5 => (SchemaChange, ByteString.empty)
    }
    (resultHeader, resultBodyRemaining)
  }
}

import java.util.UUID

// they shall be ColumnParser since they don't have to deal with remaining bytes
trait ColumnDecoders extends DecoderOps {
  implicit val uuidDecoder = Decoder[UUID] { bytes =>
    val (payload, remaining) = bytes.splitAt(16)
    val (most, least) = payload.splitAt(8)
    (new UUID(most.asByteBuffer.getLong, least.asByteBuffer.getLong), remaining)
  }

  implicit val blobDecoder = Decoder[CqlBlob] { bytes =>
    (CqlBlob(bytes), ByteString.empty)
  }
}

case class CqlBlob(bytes: ByteString)

object Decoders extends Decoders with ColumnDecoders