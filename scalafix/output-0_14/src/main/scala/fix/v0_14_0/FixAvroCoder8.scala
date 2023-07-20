
package fix.v0_14_0

import com.spotify.scio.coders.Coder
import org.apache.avro.generic.GenericRecord
import com.spotify.scio.avro._

object FixAvroCoder8 {
  val c = Coder[GenericRecord]
}
