package fix.v0_8_0

import com.spotify.scio._

object FixContextCloseExample {
  def getReminders(sc: ScioContext, input: String) = {
    sc.textFile(input)
    val result = sc.run()
  }
}
