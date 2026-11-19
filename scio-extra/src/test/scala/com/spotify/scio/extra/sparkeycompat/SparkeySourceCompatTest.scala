/*
 * Copyright 2026 Spotify AB.
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

// Deliberately outside the sparkey package, where users' code lives: the private binary
// compatibility overloads are not visible here.
package com.spotify.scio.extra.sparkeycompat

import com.spotify.scio.extra.sparkey._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.sparkey.{CompressionType, LoadMode}

class SparkeySourceCompatTest extends PipelineSpec {
  "Sparkey side inputs" should "accept Int literals for all pre-existing arguments" in {
    runWithContext { sc =>
      val kv = sc.parallelize(Seq(("a", "1"), ("b", "2")))
      val keys = sc.parallelize(Seq("a", "c"))

      val si = kv.asSparkeySideInput(
        numShards = 2,
        compressionType = CompressionType.NONE,
        compressionBlockSize = 0
      )
      val map = kv.asLargeMapSideInput(2, CompressionType.NONE, 0)
      val set = keys.asLargeSetSideInput(2, CompressionType.NONE, 0)

      val joined = kv.largeHashJoin(kv, 2, CompressionType.NONE, 0)
      val filtered = kv.largeHashIntersectByKey(keys, 2, CompressionType.NONE, 0)

      val looked = kv.keys
        .withSideInputs(si, map, set)
        .map { (k, ctx) =>
          (ctx(si).getAsString(k), ctx(map).get(k), ctx(set).contains(k))
        }
        .toSCollection
      looked should containInAnyOrder(
        Seq[(String, Option[String], Boolean)](("1", Some("1"), true), ("2", Some("2"), false))
      )
      joined should containInAnyOrder(Seq(("a", ("1", "1")), ("b", ("2", "2"))))
      filtered should containInAnyOrder(Seq(("a", "1")))
    }
  }

  it should "accept a LoadMode" in {
    runWithContext { sc =>
      val kv = sc.parallelize(Seq(("a", "1"), ("b", "2")))
      val map = kv.asLargeMapSideInput(numShards = 2, loadMode = LoadMode.NONE)
      val looked = kv.keys
        .withSideInputs(map)
        .map((k, ctx) => ctx(map).get(k))
        .toSCollection
      looked should containInAnyOrder(Seq[Option[String]](Some("1"), Some("2")))
    }
  }
}
