/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.GcpOptions

class SparkeyIT extends PipelineSpec {
  import Sparkey._

  val sideData = Seq(("a", "1"), ("b", "2"), ("c", "3"))

  "SCollection" should "support .asSparkeySideInput using default gcpTempLocation" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asSparkeySideInput
      val s = p1.withSideInputs(p2).flatMap((i, si) => si(p2).toStream.map(_._2)).toSCollection
      s should containInAnyOrder (Seq("1", "2", "3"))
    }
  }

  it should "support .asSparkeySideInput using custom gcpTempLocation" in {
    val sc = ScioContext.forTest()
    val gcpOpts = sc.optionsAs[GcpOptions]
    gcpOpts.setGcpTempLocation(gcpOpts.getGcpTempLocation + "/test-folder/sparkey-prefix")
    val p1 = sc.parallelize(Seq(1))
    val p2 = sc.parallelize(sideData).asSparkeySideInput
    val s = p1.withSideInputs(p2).flatMap((i, si) => si(p2).toStream.map(_._2)).toSCollection
    s should containInAnyOrder (Seq("1", "2", "3"))
  }
}
