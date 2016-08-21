package cassandra.streams

import akka.NotUsed
import akka.stream.scaladsl.Source

case class Row(columns: Source[Column, NotUsed])