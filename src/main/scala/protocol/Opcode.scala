package cassandra.protocol

sealed trait Opcode
object Opcode {
  case object Error extends Opcode
  case object Startup extends Opcode
  case object Ready extends Opcode
  case object Authenticate extends Opcode
  case object Options extends Opcode
  case object Supported extends Opcode
  case object Query extends Opcode
  case object Result extends Opcode
  case object Prepare extends Opcode
  case object Execute extends Opcode
  case object Register extends Opcode
  case object Event extends Opcode
  case object Batch extends Opcode
  case object AuthChallenge extends Opcode
  case object AuthResponse extends Opcode
  case object AuthSuccess extends Opcode
}