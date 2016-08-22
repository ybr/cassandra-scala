package cassandra.streams

import akka.NotUsed
import akka.util.{ByteString, Timeout}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import cassandra.decoder.{Consumed, Decoder, NotEnough}

import scala.concurrent.duration._

final class StreamDetacher[T](label: String, decoder: Decoder[T]) extends GraphStage[FlowShape[ByteString, (T, Source[ByteString, NotUsed])]] {
  val in: Inlet[ByteString] = Inlet("StreamSplitter.in")
  val out: Outlet[(T, Source[ByteString, NotUsed])] = Outlet("StreamSplitter.out")

  override val shape: FlowShape[ByteString, (T, Source[ByteString, NotUsed])] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with OutHandler with InHandler {
    private var buffer: ByteString = ByteString.empty
    private var moreBytesRequired = 0
    private var maybeSubSource: Option[SubSourceOutlet[ByteString]] = None

    override def onPush() {
      // println(s"[${label}] onPush")
      buffer ++= grab(in)

      feed()
    }

    def feed() {
      // println(s"[${label}] feed subSource = " + maybeSubSource + " buffer = " + buffer + " moreBytesRequired = " + moreBytesRequired)

      maybeSubSource match {
        case Some(subSource) =>
          if(subSource.isAvailable) {
            if(moreBytesRequired <= buffer.length) { // more than require bytes, push and complete sub source
              val (payload, remaining) = buffer.splitAt(moreBytesRequired)
              // println(s"[${label}] push to subSource payload = " + payload + " remaining = " + remaining)
              subSource.push(payload) // push to sub source its required bytes
              buffer = remaining // some bytes left
              moreBytesRequired -= payload.length
              // sub source has been completely push its required bytes
              subSource.complete()
              maybeSubSource = None
            }
            else if(buffer.length > 0) { // still missing bytes, push to sub source
              subSource.push(buffer)
              moreBytesRequired -= buffer.length
              buffer = ByteString.empty // everything has been pushed, no byte left
              // do not pull here since the sub source should pull itself on backpressure
            }
            else { // if no data to push ask upstream
              // println(s"[${label}] pull 1")
              pull(in)
            }
          }
        case None =>
          decoder.decode(buffer) match {
            case Consumed(t, remaining, requireMoreBytes) =>
              // println(s"[${label}] Consumed entity ${t} ${remaining} ${requireMoreBytes}")
              // println(s"[${label}] create sub source")
              val subSource = new SubSourceOutlet[ByteString]("SubSource")
              subSource.setHandler(subSourceHandler(subSource))

              val subFromGraph = Source.fromGraph(subSource.source)
              // println(s"[${label}] Newly created sub from graph ${subFromGraph}")
              push(out, t -> subFromGraph)

              maybeSubSource = Some(subSource)
              buffer = remaining
              moreBytesRequired = requireMoreBytes
            case NotEnough =>
              // println(s"[${label}] not enough => pull")
              pull(in) // if not enough data just pull more to feed the decoder
          }
      }
    }

    override def onPull() {
      // println(s"[${label}] onPull")
      if(buffer.isEmpty && isClosed(in)) completeStage()
      else if(buffer.length > 0) feed()
      else pull(in)
    }

    override def onDownstreamFinish(): Unit = {
      // println(s"[${label}] onDownstreamFinish")
      // completeStage()
      // Otherwise substream is open, ignore
    }

    override def onUpstreamFinish() {
      // println(s"[${label}] onUpstreamFinish => continue")
      // feed()
      // completeStage()
    }

    override def onUpstreamFailure(t: Throwable) {
      // println(s"[${label}] onUpstreamFailure " + t)
      failStage(t)
    }

    def subSourceHandler(source: SubSourceOutlet[ByteString]) = new OutHandler {
      def onPull() {
        // println(s"[${label}] subSourceHandler.onPull")
        feed()
      }
    }

    setHandlers(in, out, this)
  }
}
