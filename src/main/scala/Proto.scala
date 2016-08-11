package cassandra

import cassandra.protocol._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.io.{IO, Tcp}
import akka.pattern.ask
import akka.util.{ByteString, ByteStringBuilder}
import akka.util.Timeout

import java.net.InetSocketAddress
import java.nio.ByteOrder

import cassandra.Decoders._
import com.github.ybr.cassandra._
// import com.datastax.driver.core._
import java.util.UUID
import scala.collection.JavaConverters._

import scala.concurrent.ExecutionContext.Implicits._
import scala.concurrent.duration._

import akka._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._

sealed trait StreamState
case class Data(data: ByteString) extends StreamState
case object Exhausted extends StreamState
case object Wait extends StreamState

class FrameBodySource(f: => StreamState) extends GraphStage[SourceShape[ByteString]] {
  val out: Outlet[ByteString] = Outlet("FrameBodySource")
  val in: Inlet[ByteString] = Inlet("FrameBodySource.in ghost")

  override val shape = SourceShape(out)

  println("FrameBodySource")

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    setHandler(out, new OutHandler {
      def onPull() {
        f match {
          case Data(bs) => push(out, bs)
          case Exhausted => completeStage()
          case Wait => ()
        }
      }
    })
  }
}

class FrameParserStage extends GraphStage[FlowShape[ByteString, Frame]] {
  val in: Inlet[ByteString] = Inlet[ByteString]("FrameParser.in")
  val out: Outlet[Frame] = Outlet[Frame]("FrameParser.out")

  override val shape = FlowShape.of(in, out)

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
    var stash = ByteString.empty
    var frameBody: Option[FrameBodySource] = None
    var needed = -1

    println("FrameParserStage.createLogic")

    setHandler(out, new OutHandler {
      override def onPull(): Unit = {
        println(s"FrameParserStage.createLogic.onPull needed=${needed} frameBody=${frameBody.isDefined} stash=${stash.length}")
        if(isClosed(in)) run()
        else pull(in)
      }
    })

    setHandler(in, new InHandler {
      override def onPush(): Unit = {
        stash = stash ++ grab(in)
        println(s"FrameParserStage.createLogic.onPush needed=${needed} frameBody=${frameBody.isDefined} stash=${stash.length}")
        run()
      }

      override def onUpstreamFinish(): Unit = {
        // either we are done
        if(stash.isEmpty) completeStage()
        // or we still have bytes to emit
        // wait with completion and let run() complete when the
        // rest of the stash has been sent downstream
        else if(isAvailable(out)) run()
      }
    })

    private def run(): Unit = {
      if(needed == -1) {
        println(s"stash.length = ${stash.length}")

        decoder.FrameHeaderDecoder.frameHeader.decode(stash) match {
          case decoder.Consumed(frameHeader, remaining, _) =>
            println(s"FrameParserStage.run frame header = ${frameHeader}")
            needed = Math.max(frameHeader.length - remaining.length, -1)
            stash = remaining
            println(s"FrameParserStage.run more needed = ${needed}")
            val frameBodySource = new FrameBodySource({
              println("FrameParserStage.createLogic.run.onPull")
              if(stash.length >= 0) {
                val (emit, remaining) = stash.splitAt(needed)
                needed = needed - emit.length
                println(s"FrameParserStage.createLogic.onPush push ${emit} ${needed}")
                if(emit.length > 0) Data(emit)
                else Exhausted
              }
              else Exhausted
            })

            frameBody = Some(frameBodySource)
            push(out, Frame(frameHeader, Source.fromGraph(frameBodySource)))
            run() // cycle back to possibly already emit the next chunk
          case decoder.NotEnough => pull(in)
        }
      }
      else if(needed == 0) {
        needed = -1
        frameBody = None
      }
    }
  }
}

// CREATE KEYSPACE proto WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

object ByteStrings {
  def dump(bytes: ByteString): String = {
    def pos(i: Int) = f"$i%08x"
    def hex(b: Byte) = f" $b%02x"

    val builder = StringBuilder.newBuilder
    for(i ← 0 until bytes.size by 16) {
      val line = bytes.slice(i, i + 16)
      val (l8, r8) = line.splitAt(8)

      builder.append(pos(i))
      builder.append(' ')
      for(b ← l8) builder.append(hex(b))
      builder.append(' ')
      for(b ← r8) builder.append(hex(b))

      for(_ ← line.size to 16 * 3) builder.append(' ')

      builder.append('|')
      for(b ← line) if(b >= 32 && b <= 126) builder.append(b.toChar)
      else builder += '.'
      builder.append('|')
      builder.append('\n')
    }

    if(bytes.size % 16 != 0)
      builder
        .append(pos(bytes.size))
        .append('\n')

    builder.result()
  }
}