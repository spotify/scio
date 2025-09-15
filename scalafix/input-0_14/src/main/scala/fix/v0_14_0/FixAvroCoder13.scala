/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.ScioContext

object FixAvroCoder13 {
  val sc = ScioContext()

  val a: A = ???

  sc.parallelize(Seq(a))
}
