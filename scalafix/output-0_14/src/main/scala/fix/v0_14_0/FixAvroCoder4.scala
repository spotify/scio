
package fix.v0_14_0

import com.spotify.scio.coders.Coder
import com.spotify.scio.avro._

object FixAvroCoder4 {
  def foo[T : Coder]() = ???
  val b = foo[B]
}
