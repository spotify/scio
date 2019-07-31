/*
rule = MigrateV0_8
*/
package fix
package v0_8_0

import com.spotify.scio._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.PipelineResult.State

object FixWaitForResultExample {
  def getReminders(sc: ScioContext, input: String) = {
    val materialized = sc.textFile(input).materialize
    sc.close()
    materialized.waitForResult().value
  }
}