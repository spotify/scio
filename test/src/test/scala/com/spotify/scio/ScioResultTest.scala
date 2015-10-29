package com.spotify.scio

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.spotify.scio.testing.PipelineSpec

class ScioResultTest extends PipelineSpec {
  
  "ScioContextResult" should "reflect pipeline state" in {
    val r = runWithContext(_.parallelize(Seq(1, 2, 3)))
    r.isCompleted shouldBe true
    r.state shouldBe State.DONE
  }

}
