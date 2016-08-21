package cassandra.decoder

import akka.util.ByteString

import cassandra.protocol._

object CassandraDecoders extends CassandraDecoders

trait CassandraDecoders extends CassandraTypesDecoders {
  val frameHeader = (for {
    version <- byte
    flags <- byte
    stream <- short
    opcode <- opcode
    length <- int
  } yield FrameHeader(version, flags, stream, opcode, length)).more(_.length)

  def frameBody(opcode: Opcode): Decoder[FrameBody] = opcode match {
    case Opcode.Error => error
    case Opcode.Ready => Decoder.point(Ready)
    case Opcode.Supported => supported
    case Opcode.Result => result
  }

  val error: Decoder[Error] = tuple(int, string).map(Error.apply _ tupled)

  val supported: Decoder[Supported] = multimap.map(Supported)

  val result: Decoder[Result] = for {
    kind <- int
    header <- kind match {
      case 1 => Decoder.point(Void)
      case 2 => rows
      case 3 => setKeyspace
      case 4 => Decoder.point(Prepared)
      case 5 => Decoder.point(SchemaChange)
    }
  } yield Result(kind, header)

  def rows: Decoder[Rows] = for {
    flags <- int
    columnsCount <- int
    maybePagingState <- if((flags & 0x2) != 0) bytes.map(Some(_)) else Decoder.point(None)
    maybeGlobalTableSpec <- if((flags & 0x1) != 0) tuple(string, string).map(Some(_)) else Decoder.point(None)
    colSpecs <- (flags & 0x1) match {
      case 0 => // bit not set, col specs contains keyspace and table
        list(columnsCount)(tuple(tuple(string, string).map(Some(_)), string, columnType))
      case _ => // bit set
        list(columnsCount)(tuple(Decoder.point(None), string, columnType))
    }
    rowsCount <- int
  } yield Rows(flags, columnsCount, maybePagingState, maybeGlobalTableSpec, colSpecs, rowsCount)

  val setKeyspace: Decoder[SetKeyspace] = string.map(SetKeyspace)

  val columnType: Decoder[ColumnType] = for {
    binary <- short
  } yield binary match {
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

  val opcode: Decoder[Opcode] = byte.map {
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
}