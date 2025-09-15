/*
rule = FixAvroCoder
 */
package fix.v0_14_0

import com.spotify.scio.values.SCollection

object FixAvroCoder16 {

  def someMethod(data: SCollection[A]): SCollection[(String, A)] = {
    data.map(r => ("foo", r))
  }
}
