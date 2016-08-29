package cassandra.streams

import akka.NotUsed
import akka.actor.ActorSystem
import akka.util.ByteString
import akka.stream._
import akka.stream.scaladsl._

import cassandra.decoder._
import cassandra.protocol._

import org.scalatest._

import scala.collection.immutable
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class GroupedSpec extends FlatSpec with Matchers {
  it should "group 6 elements by 1" in {
    WithMaterializer { implicit materializer =>
      val entities = List(1, 2, 3, 4, 5, 6)

      val groups = Await.result(
        Source(entities)
        .via(new Grouped[Int](1))
        .via(Flow[Source[Int, NotUsed]].flatMapConcat(_.via(Flow[Int].fold(Vector.empty[Int])(_ :+ _))))
        .runWith(Sink.seq)
        ,
        1 second
      )

      groups should ===(Vector(
        Vector(1),
        Vector(2),
        Vector(3),
        Vector(4),
        Vector(5),
        Vector(6)
      ))
    }
  }

  it should "group 6 elements by 2" in {
    WithMaterializer { implicit materializer =>
      val entities = List(1, 2, 3, 4, 5, 6)

      val groups = Await.result(
        Source(entities)
        .via(new Grouped[Int](2))
        .via(Flow[Source[Int, NotUsed]].flatMapConcat(_.via(Flow[Int].fold(Vector.empty[Int])(_ :+ _))))
        .runWith(Sink.seq)
        ,
        1 second
      )

      groups should ===(Vector(Vector(1, 2), Vector(3, 4), Vector(5, 6)))
    }
  }

  it should "group 6 elements by 3" in {
    WithMaterializer { implicit materializer =>
      val entities = List(1, 2, 3, 4, 5, 6)

      val groups = Await.result(
        Source(entities)
        .via(new Grouped[Int](3))
        .via(Flow[Source[Int, NotUsed]].flatMapConcat(_.via(Flow[Int].fold(Vector.empty[Int])(_ :+ _))))
        .runWith(Sink.seq)
        ,
        1 second
      )

      groups should ===(Vector(Vector(1, 2, 3), Vector(4, 5, 6)))
    }
  }

  it should "group 6 elements by 6" in {
    WithMaterializer { implicit materializer =>
      val entities = List(1, 2, 3, 4, 5, 6)

      val groups = Await.result(
        Source(entities)
        .via(new Grouped[Int](6))
        .via(Flow[Source[Int, NotUsed]].flatMapConcat(_.via(Flow[Int].fold(Vector.empty[Int])(_ :+ _))))
        .runWith(Sink.seq)
        ,
        1 second
      )

      groups should ===(Vector(Vector(1, 2, 3, 4, 5, 6)))
    }
  }

  it should "upstream size does not conform to group size" in {
    WithMaterializer { implicit materializer =>
      a [IllegalStateException] should be thrownBy {
        val entities = List(1, 2, 3, 4, 5, 6)

        Await.result(
          Source(entities)
          .via(new Grouped[Int](4))
          .via(Flow[Source[Int, NotUsed]].flatMapConcat(_.via(Flow[Int].fold(Vector.empty[Int])(_ :+ _))))
          .runWith(Sink.seq)
          ,
          10 milliseconds
        )
      } 
    }
  }

  it should "bad group size result in IllegalArgumentException at construction time" in {
    WithMaterializer { implicit materializer =>
      a [IllegalArgumentException] should be thrownBy {
        new Grouped[Int](0)
      }
    }
  }
}