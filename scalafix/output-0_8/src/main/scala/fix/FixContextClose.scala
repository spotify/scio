package fix

import com.spotify.scio._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.PipelineResult.State

object FixContextCloseExample {
  def getReminders(sc: ScioContext, input: String) = {
    sc.textFile(input)
    val result = sc.run()
  }
}
