package cassandra.decoder

import akka.util.ByteString

trait Decoder[+A] { self =>
  def decode(bs: ByteString): DecoderResult[A]

  def map[B](f: A => B): Decoder[B] = new Decoder[B] {
    def decode(bs: ByteString): DecoderResult[B] = self.decode(bs) match {
      case Consumed(a, remaining, moreBytesRequired) => Consumed(f(a), remaining, moreBytesRequired)
      case NotEnough => NotEnough
    }
  }

  def flatMap[B](f: A => Decoder[B]): Decoder[B] = new Decoder[B] {
    def decode(bs: ByteString): DecoderResult[B] = self.decode(bs) match {
      case Consumed(a, remainingA, moreBytesRequiredA) => f(a).decode(remainingA) match {
        case Consumed(b, remainingB, moreBytesRequiredB) =>
          // bytes have been consumed so more bytes required decreases accordingly
          val newMoreBytesRequired = Math.max(0, moreBytesRequiredA - (bs.length - remainingB.length))
          Consumed(b, remainingB, newMoreBytesRequired)
        case NotEnough => NotEnough
      }
      case NotEnough => NotEnough
    }
  }

  def more(f: A => Int): Decoder[A] = new Decoder[A] {
    def decode(bs: ByteString): DecoderResult[A] = self.decode(bs) match {
      case Consumed(a, remaining, _) => Consumed(a, remaining, f(a))
      case NotEnough => NotEnough
    }
  }
}

object Decoder {
  def apply[A](f: ByteString => DecoderResult[A]): Decoder[A] = new Decoder[A] {
    def decode(bs: ByteString): DecoderResult[A] = f(bs) 
  }

  def point[A](a: A) = Decoder[A](Consumed(a, _, 0))

  def more(moreBytesRequired: Int): Decoder[Unit] = Decoder(bs => Consumed((), bs, moreBytesRequired))
}

sealed trait DecoderResult[+A]
case class Consumed[A](value: A, remaining: ByteString, moreBytesRequired: Int) extends DecoderResult[A]
case object NotEnough extends DecoderResult[Nothing]