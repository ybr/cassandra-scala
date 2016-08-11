package cassandra.decoder

import akka.util.ByteString

trait Decoder[A] { self =>
  def decode(bs: ByteString): DecoderResult[A]

  def map[B](f: A => B): Decoder[B] = new Decoder[B] {
    def decode(bs: ByteString): DecoderResult[B] = self.decode(bs) match {
      case Consumed(a, remaining, moreBytesRequired) => Consumed(f(a), remaining, moreBytesRequired)
      case NotEnough => NotEnough
    }
  }

  def flatMap[B](f: A => Decoder[B]): Decoder[B] = new Decoder[B] {
    def decode(bs: ByteString): DecoderResult[B] = self.decode(bs) match {
      case Consumed(a, remainingA, _) => f(a).decode(remainingA)
      case NotEnough => NotEnough
    }
  }
}

object Decoder {
  def apply[A](f: ByteString => DecoderResult[A]): Decoder[A] = new Decoder[A] {
    def decode(bs: ByteString): DecoderResult[A] = f(bs) 
  }

  def point[A](a: A) = Decoder[A](Consumed(a, _, 0))
}

sealed trait DecoderResult[+A]
case class Consumed[A](value: A, remaining: ByteString, moreBytesRequired: Int) extends DecoderResult[A]
case object NotEnough extends DecoderResult[Nothing]