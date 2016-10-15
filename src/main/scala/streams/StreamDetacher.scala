package cassandra.streams

import akka.NotUsed
import akka.util.{ByteString, Timeout}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import cassandra.decoder.{Consumed, Decoder, NotEnough}

import scala.concurrent.duration._

import utils._

sealed trait DetachResult[T]
case class Complete[T](entity: T, remaining: ByteString) extends DetachResult[T]
case class Partial[T](entity: T, remaining: Source[ByteString, NotUsed]) extends DetachResult[T]

final class StreamDetacher[T](decoder: Decoder[T])(expectedBytesCount: T => Int) extends GraphStage[FlowShape[ByteString, DetachResult[T]]] {
  val in: Inlet[ByteString] = Inlet("StreamDetacher.in")
  val out: Outlet[DetachResult[T]] = Outlet("StreamDetacher.out")

  override val shape: FlowShape[ByteString, DetachResult[T]] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with OutHandler with InHandler {
    private var buffer: ByteString = ByteString.empty
    private var moreBytesRequired = 0
    private var maybeSubSource: Option[SubSourceOutlet[ByteString]] = None

    override def onPush() {
      // println("StreamDetacher.onPush")
      buffer ++= grab(in)
      feed()
    }

    def feed() {
      // println("StreamDetacher.feed maybeSubSource = " + maybeSubSource + ", buffer = " + buffer.length + ", moreBytesRequired = " + moreBytesRequired)
      maybeSubSource match {
        case Some(subSource) =>
          if(subSource.isAvailable) {
            if(moreBytesRequired <= buffer.length) { // more than require bytes, push and complete sub source
              val (payload, remaining) = buffer.splitAt(moreBytesRequired)
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
            else pull(in) // if no data to push ask upstream
          }
        case None =>
          decoder.decode(buffer) match {
            case Consumed(t, remaining) =>
              val requireMoreBytes = expectedBytesCount(t) - (buffer.length - remaining.length) // consumed bytes
              // we have everything we need => Complete
              // println(s"remaining ${remaining.length}, requireMoreBytes = ${requireMoreBytes}")
              if(remaining.length >= requireMoreBytes) {
                val (completeBytes, overCompleteBytes) = remaining.splitAt(requireMoreBytes)
                push(out, Complete(t, completeBytes))
                buffer = overCompleteBytes
                moreBytesRequired = 0
              }
              else { // partially got the result
                val subSource = new SubSourceOutlet[ByteString]("SubSource")
                subSource.setHandler(subSourceHandler(subSource))

                val subFromGraph = Source.fromGraph(subSource.source)
                // println(s"StreamDetacher.push(out) : ${t}")
                push(out, Partial(t, subFromGraph))

                maybeSubSource = Some(subSource)
                buffer = remaining
                moreBytesRequired = requireMoreBytes
              }
            case NotEnough =>
              // upstream exhausted, we can't feed downstream any further
              if(isClosed(in)) completeStage()
              else pull(in) // if not enough data just pull more to feed the decoder
          }
      }

      // upstream exhausted and everything push downstream => completed
      if(buffer.length == 0 && isClosed(in)) completeStage()
    }

    override def onPull() {
      // println("StreamDetacher.onPull " + buffer.length + " " + isClosed(in))
      if(buffer.isEmpty && isClosed(in)) completeStage()
      else if(buffer.length > 0) feed()
      else {
        // println("StreamDetacher.pull")
        pull(in)
      }
    }

    override def onDownstreamFinish(): Unit = {
      // println("StreamDetacher.onDownstreamFinish")
      completeStage()
      // Otherwise substream is open, ignore
    }

    override def onUpstreamFinish() {
      // feed()
      // println("StreamDetacher.onUpstreamFinish")
      // nothing to push further and upstream exhausted
      if(buffer.length == 0) completeStage()
    }

    override def onUpstreamFailure(t: Throwable) {
      // println("StreamDetacher.onUpstreamFailure")
      t.printStackTrace
      failStage(t)
    }

    def subSourceHandler(source: SubSourceOutlet[ByteString]) = new OutHandler {
      def onPull() {
        // println("StreamDetacher.subSourceHandler.onPull")
        feed()
      }
    }

    setHandlers(in, out, this)
  }
}
