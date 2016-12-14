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
import com.spotify.scio.util.MultiJoin

class NamedTransformTest extends PipelineSpec {

  "ScioContext" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.withName("ReadInput").parallelize(Seq("a", "b", "c"))
      assertTransformName(p, "ReadInput/Read(InMemorySource)")
    }
  }

  "SCollection" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
        .map(_ * 3)
        .withName("OnlyEven").filter(_ % 2 == 0)
      assertTransformName(p, "OnlyEven/Filter")
    }
  }

  "DoubleSCollectionFunctions" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0, 5.0))
        .withName("CalcVariance").variance
      assertTransformName(p, "CalcVariance/Variance")
    }
  }

  "PairSCollectionFunctions" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
        .withName("SumPerKey").sumByKey
      assertTransformName(p, "SumPerKey/KvToTuple")
    }
  }

  "SCollectionWithAccumulator" should "support custom transform name" in {
    runWithContext { sc =>
      val intSum = sc.sumAccumulator[Int]("IntSum")
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
        .withAccumulator(intSum)
        .withName("TripleSum").map { (i, c) =>
          val n = i * 3
          c.addValue(intSum, n)
          n
        }
      assertTransformName(p, "TripleSum")
    }
  }

  "SCollectionWithFanout" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3)).withFanout(10)
        .withName("Sum").sum
      assertTransformName(p, "Sum/Values/Values")
    }
  }

  "SCollectionWithHotKeyFanout" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).withHotKeyFanout(10)
        .withName("Sum").sumByKey
      assertTransformName(p, "Sum/KvToTuple")
    }
  }

  "SCollectionWithSideInput" should "support custom transform name" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1, 2, 3)).asListSideInput
      val s = p1.withSideInputs(p2)
        .withName("GetX").filter((x, s) => x == "a")
      assertTransformName(s, "GetX")
    }
  }

  "SCollectionWithSideOutput" should "support custom transform name" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2)
        .withName("MakeSideOutput").map { (x, s) => s.output(p2, x + "2"); x + "1" }
      assertTransformName(main, "MakeSideOutput")
      assertTransformName(side(p2), "MakeSideOutput")
    }
  }

  "WindowedSCollection" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
        .toWindowed
        .withName("Triple").map(x => x.withValue(x.value * 3))
      assertTransformName(p, "Triple")
    }
  }

  "MultiJoin" should "support custom transform name" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = MultiJoin.withName("JoinEm").left(p1, p2)
      assertTransformName(p, "JoinEm")
    }
  }

  "Duplicate transform name" should "have number to make unique" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 5)
        .withName("MyTransform").map(_ * 2)
      val p2 = p1
        .withName("MyTransform").map(_ * 3)
      val p3 = p1
        .withName("MyTransform").map(_ * 4)
      assertTransformName(p1, "MyTransform")
      assertTransformName(p2, "MyTransform2")
      assertTransformName(p3, "MyTransform3")
    }
  }

  private def assertTransformName(p: PCollectionWrapper[_], tfName: String) =
    p.internal.getProducingTransformInternal.getFullName shouldBe tfName
}
