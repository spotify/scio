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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.testing._

class MetricsExampleTest extends PipelineSpec {

  "MetricsExample" should "work" in {
    JobTest[com.spotify.scio.examples.extra.MetricsExample.type]
      // static metrics
      .counter(MetricsExample.sum)(_.committed shouldBe Some((1 to 100).sum))
      .counter(MetricsExample.sum2)(_.committed shouldBe Some((1 to 100).sum + (1 to 50).sum))
      .counter(MetricsExample.count)(_.committed shouldBe Some(100))
      .distribution(MetricsExample.dist) { r =>
        val d = r.committed.get
        d.count() shouldBe 100
        d.min() shouldBe 1
        d.max() shouldBe 100
        d.sum() shouldBe (1 to 100).sum
        d.mean() shouldBe (1 to 100).sum / 100.0
      }
      .gauge(MetricsExample.gauge) { r =>
        val g = r.committed.get.value()
        g should be >= 1L
        g should be <= 100L
      }
      // dynamic metrics
      .counter(ScioMetrics.counter("even_2"))(_.committed shouldBe Some(1))
      .counter(ScioMetrics.counter("even_4"))(_.committed shouldBe Some(1))
      .counter(ScioMetrics.counter("even_6"))(_.committed shouldBe Some(1))
      .counter(ScioMetrics.counter("even_8"))(_.committed shouldBe Some(1))
      .run()
  }

}
