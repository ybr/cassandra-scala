package cassandra.streams

import akka.NotUsed
import akka.stream.scaladsl.Source

case class ResultSource(rows: Source[Row, NotUsed])