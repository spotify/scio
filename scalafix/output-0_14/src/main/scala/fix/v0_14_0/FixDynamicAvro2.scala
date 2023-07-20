
package fix.v0_14_0

import com.spotify.scio.io.dynamic._
import com.spotify.scio.values.SCollection
import com.spotify.scio.avro.dynamic._

object FixDynamicAvro2 {
  val scoll: SCollection[A] = ???
  val dstFn: A => String = ???
  scoll.saveAsDynamicAvroFile("")(dstFn)
}
