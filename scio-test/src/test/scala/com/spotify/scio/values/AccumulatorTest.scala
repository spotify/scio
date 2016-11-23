/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.values

import java.io.File
import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.spotify.scio._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.ScioUtil
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.options.ApplicationNameOptions

class AccumulatorTest extends PipelineSpec {

  "Accumulator" should "support accumulatorTotalValue" in {
    val sc = ScioContext.forTest()

    val maxI = sc.maxAccumulator[Int]("maxI")
    val minI = sc.minAccumulator[Int]("minI")
    val sumI = sc.sumAccumulator[Int]("sumI")
    val maxL = sc.maxAccumulator[Long]("maxL")
    val minL = sc.minAccumulator[Long]("minL")
    val sumL = sc.sumAccumulator[Long]("sumL")
    val maxD = sc.maxAccumulator[Double]("maxD")
    val minD = sc.minAccumulator[Double]("minD")
    val sumD = sc.sumAccumulator[Double]("sumD")
    sc
      .parallelize(Seq(1, 2, 3))
      .withAccumulator(maxI, minI, sumI, maxL, minL, sumL, maxD, minD, sumD)
      .map { (i, a) =>
        a.addValue(maxI, i).addValue(minI, i).addValue(sumI, i)
        a.addValue(maxL, i.toLong).addValue(minL, i.toLong).addValue(sumL, i.toLong)
        a.addValue(maxD, i.toDouble).addValue(minD, i.toDouble).addValue(sumD, i.toDouble)
        i
      }
    val r = sc.close()

    r.accumulatorTotalValue(maxI) shouldBe 3
    r.accumulatorTotalValue(minI) shouldBe 1
    r.accumulatorTotalValue(sumI) shouldBe 6
    r.accumulatorTotalValue(maxL) shouldBe 3L
    r.accumulatorTotalValue(minL) shouldBe 1L
    r.accumulatorTotalValue(sumL) shouldBe 6L
    r.accumulatorTotalValue(maxD) shouldBe 3.0
    r.accumulatorTotalValue(minD) shouldBe 1.0
    r.accumulatorTotalValue(sumD) shouldBe 6.0
    maxI.name shouldBe "maxI"
    minI.name shouldBe "minI"
    sumI.name shouldBe "sumI"
    maxL.name shouldBe "maxL"
    minL.name shouldBe "minL"
    sumL.name shouldBe "sumL"
    maxD.name shouldBe "maxD"
    minD.name shouldBe "minD"
    sumD.name shouldBe "sumD"
  }

  it should "support accumulatorValuesAtSteps" in {
    val sc = ScioContext.forTest()

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

  // scalastyle:off no.whitespace.before.left.bracket
  it should "fail on duplicate accumulator names" in {
    val msg = "requirement failed: Accumulator 'acc' already exists"
    the [IllegalArgumentException] thrownBy {
      runWithContext { sc =>
        sc.maxAccumulator[Int]("acc")
        sc.maxAccumulator[Int]("acc")
      }
    } should have message msg
    the [IllegalArgumentException] thrownBy {
      runWithContext { sc =>
        sc.minAccumulator[Int]("acc")
        sc.minAccumulator[Int]("acc")
      }
    } should have message msg
    the [IllegalArgumentException] thrownBy {
      runWithContext { sc =>
        sc.sumAccumulator[Int]("acc")
        sc.sumAccumulator[Int]("acc")
      }
    } should have message msg
  }
  // scalastyle:on no.whitespace.before.left.bracket

  it should "support saveMetrics" in {
    val sc = ScioContext.forTest()

    val maxI = sc.maxAccumulator[Int]("maxI")
    val sumL = sc.sumAccumulator[Long]("sumL")
    val sumD = sc.sumAccumulator[Double]("sumD")
    sc
      .parallelize(Seq(1, 2, 3))
      .withAccumulator(maxI, sumL, sumD)
      .map { (i, a) =>
        a.addValue(maxI, i).addValue(sumL, i.toLong).addValue(sumD, i.toDouble)
        i
      }
    val r = sc.close()

    r.accumulatorTotalValue(maxI) shouldBe 3
    r.accumulatorTotalValue(sumL) shouldBe 6L
    r.accumulatorTotalValue(sumD) shouldBe 6.0
    maxI.name shouldBe "maxI"
    sumL.name shouldBe "sumL"
    sumD.name shouldBe "sumD"

    val t = Files.createTempDirectory("saveMetrics")
    val metricsFile = new File(t.toFile, "_metrics.json")
    r.saveMetrics(metricsFile.toString)

    val mapper = ScioUtil.getScalaJsonMapper
    import com.spotify.scio.metrics._

    val metrics = mapper.readValue(metricsFile, classOf[Metrics])

    val expectedTotal = Seq(AccumulatorValue(maxI.name, r.accumulatorTotalValue(maxI)),
                            AccumulatorValue(sumL.name, r.accumulatorTotalValue(sumL)),
                            AccumulatorValue(sumD.name, r.accumulatorTotalValue(sumD)))

    val expectedSteps = Seq(
      AccumulatorStepsValue(sumD.name,
        r.accumulatorValuesAtSteps(sumD).map(e => AccumulatorStepValue(e._1, e._2))),
      AccumulatorStepsValue(maxI.name,
        r.accumulatorValuesAtSteps(maxI).map(e => AccumulatorStepValue(e._1, e._2))),
      AccumulatorStepsValue(sumL.name,
        r.accumulatorValuesAtSteps(sumL).map(e => AccumulatorStepValue(e._1, e._2))))

    metrics.version should be(scioVersion)
    metrics.scalaVersion should be(scalaVersion)
    metrics.jobName should be(sc.pipeline.getOptions.as(classOf[ApplicationNameOptions]).getAppName)
    // test run via local runner - so this will be the same a app name
    metrics.jobId should be(sc.pipeline.getOptions.as(classOf[ApplicationNameOptions]).getAppName)
    metrics.accumulators.total should contain theSameElementsAs expectedTotal
    metrics.accumulators.steps should contain theSameElementsAs expectedSteps
  }

  it should "support support unused accumulators" in {
    val sc = ScioContext.forTest()
    val maxI = sc.maxAccumulator[Int]("maxI")
    val r = sc.close()
    r.accumulatorTotalValue(maxI) should be(Integer.MIN_VALUE)
    r.accumulatorValuesAtSteps(maxI) shouldBe empty
  }

}
