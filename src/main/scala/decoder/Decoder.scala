package cassandra.decoder

import akka.util.ByteString

trait Decoder[+A] { self =>
  def decode(bs: ByteString): DecoderResult[A]

  def map[B](f: A => B): Decoder[B] = new Decoder[B] {
    def decode(bs: ByteString): DecoderResult[B] = self.decode(bs) match {
      case Consumed(a, remaining) => Consumed(f(a), remaining)
      case NotEnough => NotEnough
    }
  }

  def flatMap[B](f: A => Decoder[B]): Decoder[B] = new Decoder[B] {
    def decode(bs: ByteString): DecoderResult[B] = self.decode(bs) match {
      case Consumed(a, remainingA) => f(a).decode(remainingA)
      case NotEnough => NotEnough
    }
  }
}

object Decoder {
  def apply[A](f: ByteString => DecoderResult[A]): Decoder[A] = new Decoder[A] {
    def decode(bs: ByteString): DecoderResult[A] = f(bs) 
  }

  def point[A](a: A) = Decoder[A](Consumed(a, _))
}

sealed trait DecoderResult[+A]
case class Consumed[A](value: A, remaining: ByteString) extends DecoderResult[A]
case object NotEnough extends DecoderResult[Nothing]