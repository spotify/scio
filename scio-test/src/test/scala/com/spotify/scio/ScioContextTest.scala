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

import com.google.common.collect.Lists
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create

import scala.collection.JavaConverters._

class ScioContextTest extends PipelineSpec {

  "ScioContext" should "support applyTransform()" in {
    val sc = ScioContext()
    sc.applyTransform(Create.of((1 to 10).asJava)) should containInAnyOrder (1 to 10)
    sc.close()
  }

  it should "support pipeline" in {
    val pipeline = ScioContext().pipeline
    val p = pipeline.apply(Create.of(Lists.newArrayList(1, 2, 3)))
    PAssert.that(p).containsInAnyOrder(Lists.newArrayList(1, 2, 3))
    pipeline.run()
  }

}
