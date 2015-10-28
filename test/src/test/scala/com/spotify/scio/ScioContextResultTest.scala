package com.spotify.scio

import com.google.cloud.dataflow.sdk.PipelineResult.State
import com.spotify.scio.testing.PipelineSpec

class ScioContextResultTest extends PipelineSpec {
  
  "ScioContextResult" should "reflect pipeline state" in {
    val r = runWithContext(_.parallelize(Seq(1, 2, 3)))
    r.isCompleted shouldBe true
    r.state shouldBe State.DONE
  }

  it should "support accumulatorValue" in {
    val testId = "PipelineTest-" + System.currentTimeMillis()
    val sc = ScioContext(Array(s"--testId=$testId"))

    val max = sc.maxAccumulator[Int]("max")
    val min = sc.minAccumulator[Int]("min")
    val sum = sc.sumAccumulator[Int]("sum")
    sc
      .parallelize(Seq(1, 2, 3))
      .withAccumulator(max, min, sum)
      .map { (i, a) =>
        a.addValue(max, i).addValue(min, i).addValue(sum, i)
        i
      }
    val r = sc.close()

    r.accumulatorValue(max) should equal (3)
    r.accumulatorValue(min) should equal (1)
    r.accumulatorValue(sum) should equal (6)
  }

}
