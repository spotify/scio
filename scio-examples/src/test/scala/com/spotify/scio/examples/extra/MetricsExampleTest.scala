/*
 * Copyright 2019 Spotify AB.
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
      .counter(MetricsExample.sum)(x => x shouldBe (1 to 100).sum)
      .counter(MetricsExample.sum2)(x => x shouldBe (1 to 100).sum + (1 to 50).sum)
      .counter(MetricsExample.count)(x => x shouldBe 100)
      .distribution(MetricsExample.dist) { d =>
        d.getCount shouldBe 100
        d.getMin shouldBe 1
        d.getMax shouldBe 100
        d.getSum shouldBe (1 to 100).sum
        d.getMean shouldBe (1 to 100).sum / 100.0
      }
      .gauge(MetricsExample.gauge) { g =>
        g.getValue should be >= 1L
        g.getValue should be <= 100L
      }
      // dynamic metrics
      .counter(ScioMetrics.counter("even_2"))(x => x shouldBe 1)
      .counter(ScioMetrics.counter("even_4"))(x => x shouldBe 1)
      .counter(ScioMetrics.counter("even_6"))(x => x shouldBe 1)
      .counter(ScioMetrics.counter("even_8"))(x => x shouldBe 1)
      // context-initialized metrics
      .counter(ScioMetrics.counter("ctxcount"))(x => x shouldBe 0)
      .counter(ScioMetrics.counter("namespace", "ctxcount"))(x => x shouldBe 0)
      .run()
  }
}
