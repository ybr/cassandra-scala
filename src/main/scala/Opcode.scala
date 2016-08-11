package cassandra

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

  def fromByte(byte: Byte): Opcode = byte match {
    case 0x00 => Opcode.Error
    case 0x01 => Opcode.Startup
    case 0x02 => Opcode.Ready
    case 0x03 => Opcode.Authenticate
    case 0x05 => Opcode.Options
    case 0x06 => Opcode.Supported
    case 0x07 => Opcode.Query
    case 0x08 => Opcode.Result
    case 0x09 => Opcode.Prepare
    case 0x0A => Opcode.Execute
    case 0x0B => Opcode.Register
    case 0x0C => Opcode.Event
    case 0x0D => Opcode.Batch
    case 0x0E => Opcode.AuthChallenge
    case 0x0F => Opcode.AuthResponse
    case 0x10 => Opcode.AuthSuccess
  }
}