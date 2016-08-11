package cassandra.decoder

import akka.util.ByteIterator

import java.nio.ByteOrder

import cassandra.{BigEndian, Opcode}
import cassandra.protocol.FrameHeader

trait SizeOf[A] {
  def inBytes(): Int
}

object SizeOf {
  def apply[A](size: Int) = new SizeOf[A] {
    val inBytes = size
  }
}

trait SizeOfs {
  implicit val sizeOfDouble = SizeOf[Double](8)
  implicit val sizeOfFloat = SizeOf[Float](4)
  implicit val sizeOfLong = SizeOf[Long](8)
  implicit val sizeOfInt = SizeOf[Int](4)
  implicit val sizeOfShort = SizeOf[Short](2)
  implicit val sizeOfByte = SizeOf[Byte](1)
}

object SizeOfs extends SizeOfs

import cassandra.decoder.SizeOfs._

trait Decoders {
  def scalar[A](extract: ByteIterator => A)(implicit byteOrder: ByteOrder, size: SizeOf[A]) = Decoder[A] { bs =>
    if(bs.length >= size.inBytes) {
      val (payload, remaining) = bs.splitAt(size.inBytes)
      Consumed(extract(payload.iterator), remaining, 0)
    }
    else NotEnough
  }

  def double(implicit byteOrder: ByteOrder) = scalar[Double](_.getDouble)
  def float(implicit byteOrder: ByteOrder) = scalar[Float](_.getFloat)
  def long(implicit byteOrder: ByteOrder) = scalar[Long](_.getLong)
  def int(implicit byteOrder: ByteOrder) = scalar[Int](_.getInt)
  def short(implicit byteOrder: ByteOrder) = scalar[Short](_.getShort)
  def byte(implicit byteOrder: ByteOrder) = scalar[Byte](_.getByte)
}

import akka.util.ByteString
import cassandra._

trait CassandraDecoders extends Decoders with BigEndian {
  def list[A](size: Int)(A: Decoder[A]): Decoder[List[A]] = {
    (0 until size).foldLeft(Decoder.point(List.empty[A])) { (AS, _) =>
      for {
        a <- A
        as <- AS
      } yield a :: as
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

  val bytes: Decoder[ByteString] = for {
    size <- int
    bytes <- Decoder { bytes =>
      if(bytes.length >= size) {
        val (payload, remaining) = bytes.splitAt(size)
        Consumed(payload, remaining, 0)
      }
      else NotEnough
    }
  } yield bytes

  val string: Decoder[String] = for {
    size <- short
    bytes <- list(size)(byte)
  } yield new String(bytes.toArray, "UTF-8")

  val strings: Decoder[List[String]] = for {
    size <- short
    strings <- list(size)(string)
  } yield strings

  val multimap: Decoder[Map[String, List[String]]] = for {
    size <- short
    multilist <- list(size)(tuple(string, strings))
  } yield multilist.toMap

  val resultHeader: Decoder[ResultHeader] = for {
    kind <- int
    header <- kind match {
      case 1 => Decoder.point(Void)
      case 2 => rows
      case 3 => setKeyspace
      case 4 => Decoder.point(Prepared)
      case 5 => Decoder.point(SchemaChange)
    }
  } yield header

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
}

object CassandraDecoders extends CassandraDecoders

object FrameHeaderDecoder extends BigEndian {
  val opcode: Decoder[Opcode] = byte.map(Opcode.fromByte)

  val frameHeader = Decoder[FrameHeader] { bytes =>
    if(bytes.length >= 9) {
      val (payload, remaining) = bytes.splitAt(9)
      val reader = payload.iterator
      val version = reader.getByte
      val flags = reader.getByte
      val stream = reader.getShort
      val opcode = Opcode.fromByte(reader.getByte)
      val length = reader.getInt
      Consumed(
        FrameHeader(version, flags, stream, opcode, length),
        remaining,
        length
      )
    }
    else NotEnough
  }
}