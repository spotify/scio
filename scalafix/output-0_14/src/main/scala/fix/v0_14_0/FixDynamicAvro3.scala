
package fix.v0_14_0

import com.google.protobuf.Message
import com.spotify.scio.io.dynamic._
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro.dynamic._

object FixDynamicAvro3 {
  type X = Message
  val scoll: SCollection[X] = ???
  val dstFn: X => String = ???
  scoll.saveAsDynamicProtobufFile("")(dstFn)
}
