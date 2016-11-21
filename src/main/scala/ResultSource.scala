package com.github.ybr.cassandra

import akka.NotUsed
import akka.stream.Materializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.util.ByteString

import cassandra.decoder._
import cassandra.protocol._
import cassandra.streams._

import java.nio.ByteOrder

import scala.concurrent.{ExecutionContext, Future}

case class ResultSource(
  frameHeader: FrameHeader,
  frameBody: FrameBody,

  // bytes as they arrive
  stream: Source[ByteString, NotUsed]
) {
  def ok(): Boolean = frameBody.isInstanceOf[Result]

  def errorCode(): Option[Int] = frameBody match {
    case Error(code, _) => Some(code)
    case _ => None
  }

  def errorMessage(): Option[String] = frameBody match {
    case Error(_, message) => Some(message)
    case _ => None
  }

  def columnDefinitions(): List[ColumnDefinition] = frameBody.asInstanceOf[Result].header match {
    case header: Rows =>
      header.colSpecs.map { case (maybeTableSpec, columnName, colType) =>
        val (keyspace, tableName) = maybeTableSpec.orElse(header.globalTableSpec).get
        ColumnDefinition(keyspace, columnName, tableName, colType)
      }
    case _ => List.empty
  }

  def executionInfo(): ExecutionInfo = {
    val pagingState = frameBody.asInstanceOf[Result].header match {
      case header: Rows => header.pagingState
      case _ => None
    }
    ExecutionInfo(pagingState)
  }

  // columns as they arrive
  def columns(): Source[ColumnSource, NotUsed] = stream.via(ResultSource.columnsDetacher)

  // row sources as they arrive
  def rows(): Source[RowSource, NotUsed] = {
    columns.via(Flow.fromGraph(new Grouped[ColumnSource](frameBody.asInstanceOf[Result].header.asInstanceOf[Rows].columnsCount))).map(RowSource(columnDefinitions(), _))
  }

  // rows as they arrive
  def rowsFull()(implicit ec: ExecutionContext, mat: Materializer): Source[Row, NotUsed] = {
    rows.mapAsync(1) { row =>
      row.columns.mapAsync(1) { column =>
        column.content
        .runFold(ByteString.empty)(_ ++ _)
        .map { bytes =>
          Column(column.length, bytes)
        }
      }
      .runWith(Sink.seq)
      .map(columns => new Row(row.columnDefinitions zip columns))
    }
  }

  // Returns all the remaining rows in this ResultSet as a list.
  def all()(implicit ec: ExecutionContext, mat: Materializer): Future[Seq[Row]] = {
    println(this.errorMessage)
    val rowsHeader = frameBody.asInstanceOf[Result].header.asInstanceOf[Rows]
    val totalColumnsCount = rowsHeader.rowsCount * rowsHeader.columnsCount
    stream.runFold(ByteString.empty)(_ ++ _).map { bytes =>
      CassandraTypesDecoders.list(totalColumnsCount)(CassandraTypesDecoders.bytes).decode(bytes) match {
        case Consumed(bytesOfColumns, _) =>
          bytesOfColumns.map(bs => Column(bs.length, bs))
            .grouped(rowsHeader.columnsCount)
            .map(columns => new Row(columnDefinitions() zip columns))
            .toSeq
        case NotEnough => throw new Exception("Not enough bytes to decode all rows * columns")
      }
    }
  }

  // Returns the first row from this ResultSource.
  def one()(implicit ec: ExecutionContext, mat: Materializer): Future[Option[Row]] = rowsFull.runWith(Sink.headOption)
}

case class ColumnDefinition(
  keyspace: String,
  name: String,
  table: String,
  dataType: ColumnType
)

case class ExecutionInfo(
  pagingState: Option[ByteString]
)

object ResultSource {
  val columnsDetacher = Flow.fromGraph(new StreamDetacher(CassandraDecoders.int(ByteOrder.BIG_ENDIAN))(_ + 4)).map {
    case Partial(length, source) => ColumnSource(length, source)
    case Complete(length, bytes) => ColumnSource(length, Source.single(bytes))
  }
}