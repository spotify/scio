/*
rule = FixContextClose
 */
package fix

import com.spotify.scio._

object FixContextCloseExample {
  def getReminders(sc: ScioContext, input: String) = {
    sc.textFile(input)
    val result = sc.close()
  }
}
