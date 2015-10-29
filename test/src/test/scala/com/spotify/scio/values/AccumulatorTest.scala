package com.spotify.scio.values

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec

class AccumulatorTest extends PipelineSpec {

  "Accumulator" should "support accumulatorTotalValue" in {
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

    r.accumulatorTotalValue(max) shouldBe 3
    r.accumulatorTotalValue(min) shouldBe 1
    r.accumulatorTotalValue(sum) shouldBe 6
  }

  it should "support accumulatorValuesAtSteps" in {
    val testId = "PipelineTest-" + System.currentTimeMillis()
    val sc = ScioContext(Array(s"--testId=$testId"))

    val count = sc.sumAccumulator[Int]("count")
    sc.parallelize(1 to 100)
      .withAccumulator(count)
      .map { (i, c) =>
        c.addValue(count, 1)
        i
      }
      .filter { (i, c) =>
        val b = i % 2 == 0
        if (b) { c.addValue(count, 1) }
        b
      }
      .flatMap { (i, c) =>
        c.addValue(count, 1)
        Seq(i)
      }
    val r = sc.close()

    val av = r.accumulatorValuesAtSteps(count)
    av.size shouldBe 3
    av.find(_._1.startsWith("map@")).map(_._2) should equal (Some(100))
    av.find(_._1.startsWith("filter@")).map(_._2) should equal (Some(50))
    av.find(_._1.startsWith("flatMap@")).map(_._2) should equal (Some(50))
  }

  it should "detect duplicate accumulator names" in {
    intercept[IllegalArgumentException] {
      runWithContext { sc =>
         sc.maxAccumulator[Int]("acc")
         sc.minAccumulator[Int]("acc")
      }
    }
  }

}
