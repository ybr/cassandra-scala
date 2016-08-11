package cassandra.protocol

import akka.NotUsed
import akka.stream.scaladsl.Source
import akka.util.ByteString

case class Frame(header: FrameHeader, body: Source[ByteString, NotUsed])