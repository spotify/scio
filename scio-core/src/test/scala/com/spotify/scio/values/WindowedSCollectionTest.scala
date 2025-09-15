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

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec
import org.joda.time.Instant

class WindowedSCollectionTest extends PipelineSpec {
  "WindowedSCollection" should "support filter()" in {
    runWithContext { sc =>
      val p =
        sc.parallelizeTimestamped(Seq("a", "b", "c", "d"), Seq(1L, 2L, 3L, 4L).map(new Instant(_)))
      val r = p.toWindowed
        .filter(v => v.timestamp.getMillis % 2 == 0)
        .toSCollection
        .withTimestamp
        .map(kv => (kv._1, kv._2.getMillis))
      r should containInAnyOrder(Seq(("b", 2L), ("d", 4L)))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p =
        sc.parallelizeTimestamped(Seq("a", "b"), Seq(1L, 2L).map(new Instant(_)))
      val r = p.toWindowed
        .flatMap(v => Seq(v.copy(v.value + "1"), v.copy(v.value + "2")))
        .toSCollection
        .withTimestamp
        .map(kv => (kv._1, kv._2.getMillis))
      r should containInAnyOrder(Seq(("a1", 1L), ("a2", 1L), ("b1", 2L), ("b2", 2L)))
    }
  }

  it should "support keyBy()" in {
    runWithContext { sc =>
      val p =
        sc.parallelizeTimestamped(Seq("a", "b"), Seq(1L, 2L).map(new Instant(_)))
      val r =
        p.toWindowed.keyBy(v => v.value + v.timestamp.getMillis).toSCollection
      r should containInAnyOrder(Seq(("a1", "a"), ("b2", "b")))
    }
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p =
        sc.parallelizeTimestamped(Seq("a", "b"), Seq(1L, 2L).map(new Instant(_)))
      val r = p.toWindowed
        .map(v => v.copy(v.value + v.timestamp.getMillis))
        .toSCollection
      r should containInAnyOrder(Seq("a1", "b2"))
    }
  }
}
