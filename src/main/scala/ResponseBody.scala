package cassandra

import akka.NotUsed
import akka.util.ByteString
import akka.stream.scaladsl.Source

sealed trait NoBody
object NoBody extends NoBody

class LongString(val str: String)

case class Result(
  header: ResultHeader,
  body: Source[ByteString, NotUsed]
)

sealed trait ResultHeader
case object Void extends ResultHeader
case class Rows(
  flags: Int,
  columnsCount: Int,
  pagingState: Option[List[Byte]],
  globalTableSpec: Option[(String, String)],
  colSpecs: List[(Option[(String, String)], String, ColumnType)],
  rowsCount: Int) extends ResultHeader
case class SetKeyspace(keyspace: String) extends ResultHeader
case object Prepared extends ResultHeader
case object SchemaChange extends ResultHeader

sealed trait ColumnType
object ColumnType {
  case object Custom extends ColumnType
  case object Ascii extends ColumnType
  case object Bigint extends ColumnType
  case object Blob extends ColumnType
  case object Boolean extends ColumnType
  case object Counter extends ColumnType
  case object Decimal extends ColumnType
  case object Double extends ColumnType
  case object Float extends ColumnType
  case object Int extends ColumnType
  case object Timestamp extends ColumnType
  case object Uuid extends ColumnType
  case object Varchar extends ColumnType
  case object Varint extends ColumnType
  case object Timeuuid extends ColumnType
  case object Inet extends ColumnType
  case object List extends ColumnType
  case object Map extends ColumnType
  case object Set extends ColumnType
  case object Udt extends ColumnType
  case object Tuple extends ColumnType
}
