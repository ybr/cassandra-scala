package object utils {
  // return the result of the computation f alongside the time it took in nanoseconds
  def bench[A](f: => A): (A, Long) = {
    val start = System.nanoTime
    val result = f
    val end = System.nanoTime
    (result, end - start)
  }
}