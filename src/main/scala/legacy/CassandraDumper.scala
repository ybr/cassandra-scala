package legacy

import com.datastax.driver.core._

import java.net.InetSocketAddress
import java.nio.ByteBuffer
import java.util.UUID

import scala.util.Random

object CassandraOneDumper {
  def main(args: Array[String]) {
    val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress("192.168.99.100", 32769)).build()
    val session = cluster.connect("proto")

    println("Connected")

    val doubleCount = 300000

    val buffer = ByteBuffer.allocate(doubleCount * 8)
    (0 until doubleCount).foreach { _ =>
      val d = Random.nextDouble()
      buffer.putDouble(d)
    }
    buffer.compact()

    val jobId = UUID.randomUUID()

    println(s"Ready to save ${jobId} ${buffer}")

    session.execute("INSERT INTO one(job_id, values) VALUES(?, ?)", jobId, buffer)

    session.close()
    cluster.close()

    println("End")
  }
}

object CassandraExtractor {
  def main(args: Array[String]) {
    val cluster = Cluster.builder().addContactPointsWithPorts(new InetSocketAddress("192.168.99.100", 32769)).build()
    val session = cluster.connect("proto")

    println("Connected")

    val start = System.currentTimeMillis

    (0 to 10000).foreach(_ => get(session))

    val end = System.currentTimeMillis

    println(s"Total: ${end - start}(ms)")

    session.close()
    cluster.close()

    println("End")
  }

  def get(session: Session) {
    val start = System.currentTimeMillis
    // val rs = session.execute("SELECT values FROM one LIMIT 10")
    // val rs = session.execute("SELECT data FROM test LIMIT 300000")
    val rs = session.execute("SELECT data FROM test LIMIT 1")
    val rows = rs.all()
    val buffer = rows.get(0).getBytes(0)
    val end = System.currentTimeMillis
    // println(s"Duration ${end - start} (ms)")
    // println("Rows = " + rows.size)
    // println("OK buffer size = " + buffer.array().length)
  }
}