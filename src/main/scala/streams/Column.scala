package cassandra.streams

import akka.NotUsed
import akka.util.ByteString
import akka.stream.scaladsl.Source

case class Column(content: Source[ByteString, NotUsed])