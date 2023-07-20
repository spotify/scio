/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.coders.Coder

object FixAvroCoder3 {
  def foo[T : Coder]() = ???
  val a = foo[A]
}
