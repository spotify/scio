/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.ScioContext

object FixAvroCoder14 {
  val sc = ScioContext()

  val elements = Seq(new A)
  sc.parallelize(elements)
}
