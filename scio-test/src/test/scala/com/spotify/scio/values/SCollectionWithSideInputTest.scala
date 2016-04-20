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

import com.spotify.scio.testing.PipelineSpec

class SCollectionWithSideInputTest extends PipelineSpec {

  val sideData = Seq(("a", 1), ("b", 2), ("c", 3))

  "SCollectionWithSideInput" should "support asSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(Seq(sideData)).asSingletonSideInput
      val s = p1.withSideInputs(p2).map((i, s) => (i, s(p2))).toSCollection
      s should containSingleValue ((1, sideData))
    }
  }

  it should "support asListSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asListSideInput
      val s = p1.withSideInputs(p2).flatMap((i, s) => s(p2)).toSCollection
      s should containInAnyOrder (sideData)
    }
  }

  it should "support asIterableSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asIterableSideInput
      val s = p1.withSideInputs(p2).flatMap((i, s) => s(p2)).toSCollection
      s should containInAnyOrder (sideData)
    }
  }

  it should "support asMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asMapSideInput
      val s = p1.withSideInputs(p2).flatMap((i, s) => s(p2).toSeq).toSCollection
      s should containInAnyOrder (sideData)
    }
  }

  it should "support asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 =
        sc.parallelize(sideData ++ sideData.map(kv => (kv._1, kv._2 + 10))).asMultiMapSideInput
      val s = p1.withSideInputs(p2).flatMap((i, s) => s(p2).mapValues(_.toSet)).toSCollection
      s should containInAnyOrder (sideData.map(kv => (kv._1, Set(kv._2, kv._2 + 10))))
    }
  }

  it should "support filter()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 10)
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val s = p1.withSideInputs(p2).filter((x, s) => (x + s(p2)) % 2 == 0).toSCollection
      s should containInAnyOrder (Seq(1, 3, 5, 7, 9))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1, 2)).asListSideInput
      val s = p1.withSideInputs(p2).flatMap((x, s) => s(p2).map(x + _)).toSCollection
      s should containInAnyOrder (Seq("a1", "b1", "c1", "a2", "b2", "c2"))
    }
  }

  it should "support keyBy()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val s = p1.withSideInputs(p2).keyBy(_ + _(p2)).toSCollection
      s should containInAnyOrder (Seq(("a1", "a"), ("b1", "b"), ("c1", "c")))
    }
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val s = p1.withSideInputs(p2).map(_ + _(p2)).toSCollection
      s should containInAnyOrder (Seq("a1", "b1", "c1"))
    }
  }

  it should "support chaining" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asListSideInput
      val p4 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).asMultiMapSideInput
      val s1 = p1.withSideInputs(p2)
        .filter((x, s) => x == "a")
        .flatMap((x, s) => Seq(x + s(p2) + "x", x + s(p2) + "y"))
        .keyBy((x, s) => "k" + s(p2))
        .map((kv, s) => (kv._1, kv._2, s(p2)))
        .toSCollection
      val s2 = p1.withSideInputs(p3)
        .filter((x, s) => x == "a")
        .flatMap((x, s) => Seq(x + s(p3).sum + "x", x + s(p3).sum + "y"))
        .keyBy((x, s) => "k" + s(p3).sum)
        .map((kv, s) => (kv._1, kv._2, s(p3).sum))
        .toSCollection
      val s3 = p1.withSideInputs(p4)
        .filter((x, s) => s(p4)(x).sum == 1)
        .flatMap((x, s) => Seq(x + s(p4)(x).sum + "x", x + s(p4)(x).sum + "y"))
        .keyBy((x, s) => "k" + s(p4).values.flatten.sum)
        .map((kv, s) => (kv._1, kv._2, s(p4).values.flatten.sum))
        .toSCollection
      s1 should containInAnyOrder (Seq(("k1", "a1x", 1), ("k1", "a1y", 1)))
      s2 should containInAnyOrder (Seq(("k6", "a6x", 6), ("k6", "a6y", 6)))
      s3 should containInAnyOrder (Seq(("k6", "a1x", 6), ("k6", "a1y", 6)))
    }
  }

}
