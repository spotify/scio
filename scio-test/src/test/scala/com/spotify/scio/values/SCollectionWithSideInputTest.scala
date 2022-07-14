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
import org.apache.beam.sdk.transforms.View
import org.joda.time.{DateTimeConstants, Duration, Instant}

class SCollectionWithSideInputTest extends PipelineSpec {
  val sideData: Seq[(String, Int)] = Seq(("a", 1), ("b", 2), ("c", 3))

  "SCollectionWithSideInput" should "support asSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(Seq(sideData)).asSingletonSideInput
      val s = p1.withSideInputs(p2).map((i, s) => (i, s(p2))).toSCollection
      s should containSingleValue((1, sideData))
    }
  }

  it should "support asSingletonSideInput with default value" in {
    runWithContext { sc =>
      val defSideInput = Seq(("a", 1))
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc
        .parallelize(Option.empty[Seq[(String, Int)]])
        .asSingletonSideInput(defSideInput)
      val s = p1.withSideInputs(p2).map((i, s) => (i, s(p2))).toSCollection
      s should containSingleValue((1, defSideInput))
    }
  }

  it should "support asListSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asListSideInput
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2)).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "support asIterableSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asIterableSideInput
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2)).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "support asSetSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData ++ sideData).asSetSingletonSideInput
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2)).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "support asMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asMapSideInput
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2).toSeq).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "support asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 =
        sc.parallelize(sideData ++ sideData.map(kv => (kv._1, kv._2 + 10))).asMultiMapSideInput
      val s = p1
        .withSideInputs(p2)
        .flatMap((_, s) => s(p2).iterator.map { case (k, v) => (k, v.toSet) }.toMap)
        .toSCollection

      s should containInAnyOrder(sideData.map(kv => (kv._1, Set(kv._2, kv._2 + 10))))
    }
  }

  it should "support asMapSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asMapSingletonSideInput
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2).toSeq).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "support asMultiMapSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 =
        sc.parallelize(sideData ++ sideData.map(kv => (kv._1, kv._2 + 10)))
          .asMultiMapSingletonSideInput
      val s = p1
        .withSideInputs(p2)
        .flatMap((_, s) => s(p2).map { case (k, v) => k -> v.toSet })
        .toSCollection
      s should containInAnyOrder(sideData.map(kv => (kv._1, Set(kv._2, kv._2 + 10))))
    }
  }

  it should "support filter()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 10)
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val s = p1
        .withSideInputs(p2)
        .filter((x, s) => (x + s(p2)) % 2 == 0)
        .toSCollection
      s should containInAnyOrder(Seq(1, 3, 5, 7, 9))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1, 2)).asListSideInput
      val s =
        p1.withSideInputs(p2).flatMap((x, s) => s(p2).map(x + _)).toSCollection
      s should containInAnyOrder(Seq("a1", "b1", "c1", "a2", "b2", "c2"))
    }
  }

  it should "support keyBy()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val s = p1.withSideInputs(p2).keyBy(_ + _(p2)).toSCollection
      s should containInAnyOrder(Seq(("a1", "a"), ("b1", "b"), ("c1", "c")))
    }
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val s = p1.withSideInputs(p2).map(_ + _(p2)).toSCollection
      s should containInAnyOrder(Seq("a1", "b1", "c1"))
    }
  }

  it should "support chaining" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1)).asSingletonSideInput
      val p3 = sc.parallelize(Seq(1, 2, 3)).asListSideInput
      val p4 =
        sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).asMultiMapSideInput
      val s1 = p1
        .withSideInputs(p2)
        .filter((x, _) => x == "a")
        .flatMap((x, s) => Seq(x + s(p2) + "x", x + s(p2) + "y"))
        .keyBy((_, s) => "k" + s(p2))
        .map((kv, s) => (kv._1, kv._2, s(p2)))
        .toSCollection
      val s2 = p1
        .withSideInputs(p3)
        .filter((x, _) => x == "a")
        .flatMap((x, s) => Seq(x + s(p3).sum + "x", x + s(p3).sum + "y"))
        .keyBy((_, s) => "k" + s(p3).sum)
        .map((kv, s) => (kv._1, kv._2, s(p3).sum))
        .toSCollection
      val s3 = p1
        .withSideInputs(p4)
        .filter((x, s) => s(p4)(x).sum == 1)
        .flatMap((x, s) => Seq(x + s(p4)(x).sum + "x", x + s(p4)(x).sum + "y"))
        .keyBy((_, s) => "k" + s(p4).values.flatten.sum)
        .map((kv, s) => (kv._1, kv._2, s(p4).values.flatten.sum))
        .toSCollection
      s1 should containInAnyOrder(Seq(("k1", "a1x", 1), ("k1", "a1y", 1)))
      s2 should containInAnyOrder(Seq(("k6", "a6x", 6), ("k6", "a6y", 6)))
      s3 should containInAnyOrder(Seq(("k6", "a1x", 6), ("k6", "a1y", 6)))
    }
  }

  val timestampedData: IndexedSeq[(Int, Instant)] =
    (1 to 100).map(x => (x, new Instant(x.toLong * DateTimeConstants.MILLIS_PER_SECOND)))

  it should "support windowed asSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .asSingletonSideInput
      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Int)](t => t._1 == t._2)
    }
  }

  it should "support windowed asListSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .flatMap(x => 1 to x)
        .asListSideInput
      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Seq[Int])](t => (1 to t._1).toSet == t._2.toSet)
    }
  }

  it should "support windowed asIterableSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .flatMap(x => 1 to x)
        .asIterableSideInput
      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Iterable[Int])](t => (1 to t._1).toSet == t._2.toSet)
    }
  }

  it should "support windowed asSetSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData ++ timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .flatMap(x => 1 to x)
        .asSetSingletonSideInput
      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Set[Int])](t => (1 to t._1).toSet == t._2)
    }
  }

  it should "support windowed asMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .flatMap(x => (1 to x).map(_ -> x))
        .asMapSideInput
      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Map[Int, Int])](t => (1 to t._1).map(_ -> t._1).toMap == t._2)
    }
  }

  it should "support windowed asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .flatMap(x => (1 to x).map(x -> _))
        .asMultiMapSideInput

      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Map[Int, Iterable[Int]])] { t =>
        Map(t._1 -> (1 to t._1).toSet) == t._2.iterator.map { case (k, v) => k -> v.toSet }.toMap
      }
    }
  }

  "SideInput" should "allow to wrap a view of a Singleton" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val i2 = sc.parallelize(Seq(sideData)).internal.apply(View.asSingleton())
      val p2 = SideInput.wrapSingleton(i2)
      val s = p1.withSideInputs(p2).map((i, s) => (i, s(p2))).toSCollection
      s should containSingleValue((1, sideData))
    }
  }

  it should "allow to wrap a view of a List" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val i2 = sc.parallelize(sideData).internal.apply(View.asList())
      val p2 = SideInput.wrapList(i2)
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2)).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "allow to wrap a view of a Iterable" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val i2 = sc.parallelize(sideData).internal.apply(View.asIterable())
      val p2 = SideInput.wrapIterable(i2)
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2)).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "allow to wrap a view of a Map" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val i2 = sc.parallelize(sideData).toKV.internal.apply(View.asMap())
      val p2 = SideInput.wrapMap(i2)
      val s = p1.withSideInputs(p2).flatMap((_, s) => s(p2).toSeq).toSCollection
      s should containInAnyOrder(sideData)
    }
  }

  it should "allow to wrap a view of a MultiMap" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val i2 =
        sc.parallelize(sideData ++ sideData.map(kv => (kv._1, kv._2 + 10)))
          .toKV
          .internal
          .apply(View.asMultimap())
      val p2 = SideInput.wrapMultiMap(i2)
      val s = p1
        .withSideInputs(p2)
        .flatMap((_, s) => s(p2).map { case (k, v) => k -> v.toSet })
        .toSCollection
      s should containInAnyOrder(sideData.map(kv => (kv._1, Set(kv._2, kv._2 + 10))))
    }
  }

  it should "allow mapping over a SingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(Seq(sideData)).asSingletonSideInput
      val p3 = p2.map(seq => seq.map { case (k, v) => (k, v * 2) })
      val s = p1.withSideInputs(p3).map((i, s) => (i, s(p3))).toSCollection
      s should containSingleValue((1, sideData.map { case (k, v) => (k, v * 2) }))
    }
  }

  it should "allow mapping over a ListSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(1))
      val p2 = sc.parallelize(sideData).asListSideInput
      val p3 = p2.map(seq => seq.map { case (k, v) => (k, v * 2) }.toSet)
      val s = p1.withSideInputs(p3).map((i, s) => (i, s(p3))).toSCollection
      s should containSingleValue((1, sideData.map { case (k, v) => (k, v * 2) }.toSet))
    }
  }

  it should "support windowed and mapped asListSideInput" in {
    runWithContext { sc =>
      val p1 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
      val p2 = sc
        .parallelizeTimestamped(timestampedData)
        .withFixedWindows(Duration.standardSeconds(1))
        .flatMap(x => 1 to x)
        .asListSideInput
        .map(seq => seq.map(_ * 2))
      val s = p1.withSideInputs(p2).map((x, s) => (x, s(p2))).toSCollection
      s should forAll[(Int, Seq[Int])](t => (1 to t._1).map(_ * 2).toSet == t._2.toSet)
    }
  }
}
