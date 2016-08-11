package cassandra

sealed trait CompressionAlgorithm
object LZ4 extends CompressionAlgorithm
object Snappy extends CompressionAlgorithm