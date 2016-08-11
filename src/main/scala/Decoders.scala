package cassandra

// import akka.util.ByteString

// import java.util.UUID

// // they shall be ColumnParser since they don't have to deal with remaining bytes
// trait ColumnDecoders extends DecoderOps {
//   implicit val uuidDecoder = Decoder[UUID] { bytes =>
//     val (payload, remaining) = bytes.splitAt(16)
//     val (most, least) = payload.splitAt(8)
//     (new UUID(most.asByteBuffer.getLong, least.asByteBuffer.getLong), remaining)
//   }

//   implicit val blobDecoder = Decoder[CqlBlob] { bytes =>
//     (CqlBlob(bytes), ByteString.empty)
//   }
// }

// case class CqlBlob(bytes: ByteString)