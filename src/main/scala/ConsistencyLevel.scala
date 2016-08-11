package cassandra

sealed trait ConsistencyLevel
object Any extends ConsistencyLevel
object One extends ConsistencyLevel
object Two extends ConsistencyLevel
object Three extends ConsistencyLevel
object Quorum extends ConsistencyLevel
object All extends ConsistencyLevel
object LocalQuorum extends ConsistencyLevel
object EachQuorum extends ConsistencyLevel
object Serial extends ConsistencyLevel
object LocalSerial extends ConsistencyLevel
object LocalOne extends ConsistencyLevel