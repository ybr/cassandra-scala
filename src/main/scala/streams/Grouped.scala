package cassandra.streams

import akka.NotUsed
import akka.util.{ByteString, Timeout}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import cassandra.decoder.{Consumed, Decoder, NotEnough}

import scala.concurrent.duration._

final class Grouped[T](n: Int) extends GraphStage[FlowShape[T, Source[T, NotUsed]]] {
  if(n <= 0) throw new IllegalArgumentException(s"The group size must a int > 0, actual value is ${n}")

  val in: Inlet[T] = Inlet("Group.in")
  val out: Outlet[Source[T, NotUsed]] = Outlet("Group.out")

  // println(s"Gouped($n)")

  override val shape = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with OutHandler with InHandler {
    private var counter: Long = 0
    private var maybeSubSource: Option[SubSourceOutlet[T]] = None
    private var maybeEntity: Option[T] = None

    def feed() {
      // println(s"feed counter = ${counter}, maybeSubSource = ${maybeSubSource}, maybeEntity = ${maybeEntity}")
      maybeSubSource match {
        case Some(subSource) =>
          if(subSource.isAvailable) {
            maybeEntity match {
              case Some(entity) =>
                subSource.push(entity)
                counter += 1
                maybeEntity = None

                if(counter % n == 0) {
                  subSource.complete()
                  maybeSubSource = None
                }
              case None => pull(in)
            }
          }
        case None => 
          // println("Create sub source")
          val subSource = new SubSourceOutlet[T]("SubSource")
          subSource.setHandler(subSourceHandler)
          push(out, Source.fromGraph(subSource.source))
          maybeSubSource = Some(subSource)
      }
    }

    override def onPush() {
      // println("Group.onPush")
      maybeEntity = Some(grab(in))
      feed()
    }

    override def onPull() {
      // println("Group.onPull")
      feed()
    }

    override def onDownstreamFinish(): Unit = {
      // println("Group.onDownstreamFinish")
      completeStage()
      // Otherwise substream is open, ignore
    }

    override def onUpstreamFinish() {
      if(counter % n == 0) {
        // stream count conforms to group size
        completeStage()
      }
      else {
        // stream count does not conform to group size
        failStage(new IllegalStateException(s"The upstream is exhausted and it count of elements ${counter} does not conform to group size ${n}"))
      }
    }

    override def onUpstreamFailure(t: Throwable) {
      // println("Group.onUpstreamFailure")
      failStage(t)
    }

    def subSourceHandler() = new OutHandler {
      def onPull() {
        // println("Group.subSourceHandler.onPull")
        feed()
      }
    }

    setHandlers(in, out, this)
  }
}
