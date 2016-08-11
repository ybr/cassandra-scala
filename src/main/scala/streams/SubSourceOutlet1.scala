package cassandra.streams

import akka.NotUsed
import akka.stream._
import akka.stream.stage._
import akka.stream.impl.fusing._

import scala.concurrent.duration._

class SubSourceOutlet1[T](name: String) {
  private var handler: OutHandler = null
  private var available = false
  private var closed = false

  // the only difference with the original one lies here
  private val callback = new AsyncCallback[SubSink.Command] {
    override def invoke(event: SubSink.Command): Unit = {
      event match {
        case SubSink.RequestOne ⇒
          if (!closed) {
            available = true
            handler.onPull()
          }
        case SubSink.Cancel ⇒
          if (!closed) {
            available = false
            closed = true
            handler.onDownstreamFinish()
          }
      }
    }
  }

  private val _source = new SubSource[T](name, callback)

  /**
   * Set the source into timed-out mode if it has not yet been materialized.
   */
  def timeout(d: FiniteDuration): Unit =
    if (_source.timeout(d)) closed = true

  /**
   * Get the Source for this dynamic output port.
   */
  def source: Graph[SourceShape[T], NotUsed] = _source

  /**
   * Set OutHandler for this dynamic output port; this needs to be done before
   * the first substream callback can arrive.
   */
  def setHandler(handler: OutHandler): Unit = this.handler = handler

  /**
   * Returns `true` if this output port can be pushed.
   */
  def isAvailable: Boolean = available

  /**
   * Returns `true` if this output port is closed, but caution
   * THIS WORKS DIFFERENTLY THAN THE NORMAL isClosed(out).
   * Due to possibly asynchronous shutdown it may not return
   * `true` immediately after `complete()` or `fail()` have returned.
   */
  def isClosed: Boolean = closed

  /**
   * Push to this output port.
   */
  def push(elem: T): Unit = {
    available = false
    _source.pushSubstream(elem)
  }

  /**
   * Complete this output port.
   */
  def complete(): Unit = {
    available = false
    closed = true
    _source.completeSubstream()
  }

  /**
   * Fail this output port.
   */
  def fail(ex: Throwable): Unit = {
    available = false
    closed = true
    _source.failSubstream(ex)
  }

  override def toString = s"SubSourceOutlet($name)"
}