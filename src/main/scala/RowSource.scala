package com.github.ybr.cassandra

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString

import java.util.{Date, UUID}

import cassandra.protocol.ColumnType

case class RowSource(columnDefinitions: List[ColumnDefinition], columns: Source[ColumnSource, NotUsed])

class Row(columns: List[(ColumnDefinition, Column)]) {
  def columnAs[A](index: Int, desiredColumnType: ColumnType)(f: ByteString => A): Option[A] = {
    val (definition, column) = columns(index)
    if(definition.dataType != desiredColumnType) throw new BadColumnType(s"Column ${index} definition does not conform with the column type ${desiredColumnType}, actual ${definition.dataType}", definition, desiredColumnType)
    else {
      if(column.length < 0) None
      else Some(f(column.bytes))
    }
  }

  def columnAs[A](columnName: String, desiredColumnType: ColumnType)(f: ByteString => A): Option[A] = {
    val (definition, column) = columns.find(_._1.name == columnName).getOrElse(throw new UnknownColumn(s"Column ${columnName} does not exist in this row", columnName))
    if(definition.dataType != desiredColumnType) throw new BadColumnType(s"Column ${columnName} definition does not conform with the column type ${desiredColumnType}, actual ${definition.dataType}", definition, desiredColumnType)
    else {
      if(column.length < 0) None
      else Some(f(column.bytes))
    }
  }

  def bool(index: Int): Option[Boolean] = columnAs(index, ColumnType.Boolean)(_.asByteBuffer.get() != 0)
  def bool(columnName: String): Option[Boolean] = columnAs(columnName, ColumnType.Boolean)(_.asByteBuffer.get() != 0)

  def blob(index: Int): Option[ByteString] = columnAs(index, ColumnType.Blob)(identity)
  def blob(columnName: String): Option[ByteString] = columnAs(columnName, ColumnType.Blob)(identity)

  def date(index: Int): Option[Date] = columnAs(index, ColumnType.Timestamp)(???)
  def date(columnName: String): Option[Date] = columnAs(columnName, ColumnType.Timestamp)(???)

  // decimal: BigDecimal
  // double
  // float
  // inet: java.net.InetAddress
  // int
  // list
  // long
  // map
  // set
  def string(index: Int): Option[String] = columnAs(index, ColumnType.Ascii)(_.decodeString("UTF-8"))
  def string(columnName: String): Option[String] = columnAs(columnName, ColumnType.Ascii)(_.decodeString("UTF-8"))
 
  def uuid(index: Int): Option[UUID] = columnAs(index, ColumnType.Uuid) { bs =>
    val buffer = bs.asByteBuffer
    val mostSigBits = buffer.getLong()
    val leastSigBits = buffer.getLong()
    new UUID(mostSigBits, leastSigBits)
  }
  def uuid(columnName: String): Option[UUID] = columnAs(columnName, ColumnType.Uuid) { bs =>
    val buffer = bs.asByteBuffer
    val mostSigBits = buffer.getLong()
    val leastSigBits = buffer.getLong()
    new UUID(mostSigBits, leastSigBits)
  }
  // varint: BigInteger
}

case class Column(length: Int, bytes: ByteString)
case class ColumnSource(length: Int, content: Source[ByteString, NotUsed])

class BadColumnType(message: String, definition: ColumnDefinition, desiredColumnType: ColumnType) extends Exception(message)
class UnknownColumn(message: String, columnName: String) extends Exception(message)