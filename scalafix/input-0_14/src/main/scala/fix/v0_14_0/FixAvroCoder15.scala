/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import org.apache.avro.specific.SpecificRecord
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder

object FixAvroCoder15 {

  def someMethod[T <: SpecificRecord : Coder](someParam: String, t: T): Unit = ???

  val sc = ScioContext()

  someMethod("foo", new A)
}
