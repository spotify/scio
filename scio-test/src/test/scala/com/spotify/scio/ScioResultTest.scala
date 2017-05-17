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

package com.spotify.scio

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.PipelineResult.State

class ScioResultTest extends PipelineSpec {

  "ScioContextResult" should "reflect pipeline state" in {
    val r = runWithContext(_.parallelize(Seq(1, 2, 3)))
    r.isCompleted shouldBe true
    r.state shouldBe State.DONE
  }

  it should "not support retrieval of accumulators during template creation" in {
    val sc = ScioContext.forTest()
    sc.optionsAs[DataflowPipelineOptions].setTemplateLocation("gs://bucket/path")
    val maxI = sc.maxAccumulator[Int]("maxI")
    sc.parallelize(Seq(1, 2, 3))
      .withAccumulator(maxI)
      .map { (i, a) =>
        a.addValue(maxI, i)
        i
      }
    val r = sc.close()
    the[IllegalArgumentException] thrownBy(r.accumulatorTotalValue(maxI)) should have message(
      "requirement failed: Cannot retrieve accumulator values during template creation"
    )
  }

}
