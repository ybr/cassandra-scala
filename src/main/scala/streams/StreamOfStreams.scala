package cassandra.streams

import akka.NotUsed
import akka.util.{ByteString, Timeout}
import akka.stream.{ActorMaterializer, Attributes, FlowShape, Inlet, Outlet}
import akka.stream.scaladsl.Source
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic}

import cassandra.decoder.{Consumed, Decoder, NotEnough}

import scala.concurrent.duration._

final class StreamSplitter[T](decoder: Decoder[T]) extends GraphStage[FlowShape[ByteString, (T, Source[ByteString, NotUsed])]] {
  val in: Inlet[ByteString] = Inlet("StreamSplitter.in")
  val out: Outlet[(T, Source[ByteString, NotUsed])] = Outlet("StreamSplitter.out")

  override val shape: FlowShape[ByteString, (T, Source[ByteString, NotUsed])] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new TimerGraphStageLogic(shape) with OutHandler with InHandler {
    private val SubscriptionTimer = "SubstreamSubscriptionTimer"

    private var buffer: ByteString = ByteString.empty
    private var requireMoreBytes = 0

    private var maybeSource: Option[SubSourceOutlet1[ByteString]] = None

    private val timeout = 10 second

    override protected def onTimer(timerKey: Any): Unit = {
      println("onTimer " + timerKey)
    }

    override def onPush() {
      println("StreamSplitter.onPush")
      buffer ++= grab(in)
      println("StreamSplitter.onPush buffer = " + buffer)
      maybeSource match {
        case Some(source) if !source.isClosed && buffer.length < requireMoreBytes =>
          println("SOURCE PUSH BUFFER")
          println("REQUIRE MORE BYTES")
          source.push(buffer)
          requireMoreBytes = requireMoreBytes - buffer.length
          buffer = ByteString.empty
        case Some(source) if !source.isClosed && buffer.length >= requireMoreBytes =>
          println("HAS ENOUGH BYTES")
          val (bytesToPush, tooManyBytes) = buffer.splitAt(requireMoreBytes)
          source.push(bytesToPush)
          source.complete()
          maybeSource = None
          buffer = tooManyBytes
        case _ =>
          println("No sub source")
          println("DECODED = " + decoder.decode(buffer))
          decoder.decode(buffer) match {
            case Consumed(t, remaining, moreBytesRequired) =>
              val subSource = new SubSourceOutlet1[ByteString]("SubSource")
              subSource.setHandler(subHandler)
              maybeSource = Some(subSource)
              setKeepGoing(true)
              scheduleOnce(SubscriptionTimer, timeout)
              push(out, t -> Source.fromGraph(subSource.source))
              requireMoreBytes = moreBytesRequired - remaining.length
              buffer = remaining
              println("buffer 0 = " + buffer + " requireMoreBytes = " + requireMoreBytes)
            case NotEnough =>
              pull(in) // if I pull on a push event wouldn't I bypass back-pressure ???
          }
      }
    }

    override def onPull() {
      println("StreamSplitter.onPull")
      if(isClosed(in)) {
        completeStage()
      }
      else {
        println("buffer 1 = " + buffer)
        maybeSource.foreach { source =>
          println("signet 0 " + (buffer.length > 0) + " " + !source.isClosed + " " + source.isAvailable)
          if(buffer.length > 0 && !source.isClosed && source.isAvailable) {
            println("signet 1")
            println("SOURCE PUSH BUFFER")
            source.push(buffer)
            buffer = ByteString.empty
          }
        }

        if(!isClosed(in)) {
          println("onPull pull")
          pull(in)
        }
      }
    }

    override def onUpstreamFinish() {
      maybeSource.filterNot(_.isClosed).foreach(_.complete())
      completeStage()
    }

    override def onUpstreamFailure(t: Throwable) {
      maybeSource match {
        case Some(source) if !source.isClosed =>
          source.fail(t)
          completeStage()
        case _ => failStage(t)
      }
    }

    private def subHandler = new OutHandler {
      override def onPull(): Unit = {
        println("subHandler 0")
        setKeepGoing(false)
        cancelTimer(SubscriptionTimer)
        // if(!buffer.isEmpty) {
        //   maybeSource.get.push(buffer)

        //   if(requireMoreBytes == 0) maybeSource.get.complete()
        //   buffer = ByteString.empty
        // }
        // if(isAvailable(in)) 
        pull(in)
        println("subHandler 0 pulled")
        // maybeSource.get.setHandler(new OutHandler {
        //   override def onPull(): Unit = {
        //     println("subHandler 1")
        //     pull(in)
        //   }
        // })
      }
    }

     setHandlers(in, out, this)
  }
}