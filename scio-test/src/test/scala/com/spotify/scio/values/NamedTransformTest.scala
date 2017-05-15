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
      assertTransformNameStartsWith(p, "ReadInput/Read")
    }
  }

  "SCollection" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
        .map(_ * 3)
        .withName("OnlyEven").filter(_ % 2 == 0)
      assertTransformNameStartsWith(p, "OnlyEven")
    }
  }

  "DoubleSCollectionFunctions" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1.0, 2.0, 3.0, 4.0, 5.0))
        .withName("CalcVariance").variance
      assertTransformNameStartsWith(p, "CalcVariance")
    }
  }

  "PairSCollectionFunctions" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
        .withName("SumPerKey").sumByKey
      assertTransformNameStartsWith(p, "SumPerKey/KvToTuple")
    }
  }

  "SCollectionWithFanout" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3)).withFanout(10)
        .withName("Sum").sum
      assertTransformNameStartsWith(p, "Sum/Values/Values")
    }
  }

  "SCollectionWithHotKeyFanout" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3))).withHotKeyFanout(10)
        .withName("Sum").sumByKey
      assertTransformNameStartsWith(p, "Sum/KvToTuple")
    }
  }

  "SCollectionWithSideInput" should "support custom transform name" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = sc.parallelize(Seq(1, 2, 3)).asListSideInput
      val s = p1.withSideInputs(p2)
        .withName("GetX").filter((x, s) => x == "a")
      assertTransformNameStartsWith(s, "GetX")
    }
  }

  "SCollectionWithSideOutput" should "support custom transform name" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2)
        .withName("MakeSideOutput").map { (x, s) => s.output(p2, x + "2"); x + "1" }
      assertTransformNameStartsWith(main, "MakeSideOutput")
      assertTransformNameStartsWith(side(p2), "MakeSideOutput")
    }
  }

  "WindowedSCollection" should "support custom transform name" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5))
        .toWindowed
        .withName("Triple").map(x => x.withValue(x.value * 3))
      assertTransformNameStartsWith(p, "Triple")
    }
  }

  "MultiJoin" should "support custom transform name" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = MultiJoin.withName("JoinEm").left(p1, p2)
      assertTransformNameStartsWith(p, "JoinEm")
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
      assertTransformNameStartsWith(p1, "MyTransform")
      assertTransformNameStartsWith(p2, "MyTransform2")
      assertTransformNameStartsWith(p3, "MyTransform3")
    }
  }

  "TransformNameable" should "prevent repeated calls to .withName" in {
    // scalastyle:off no.whitespace.before.left.bracket
    val e = the [IllegalArgumentException] thrownBy {
      runWithContext { sc =>
        val p1 = sc.parallelize(1 to 5).withName("Double").withName("DoubleMap").map(_ * 2)
      }
    }
    // scalastyle:on no.whitespace.before.left.bracket
    val msg = "requirement failed: withName() has already been used to set 'Double' as " +
      "the name for the next transform."
    e should have message msg
  }

  private def assertTransformNameStartsWith(p: PCollectionWrapper[_], tfName: String) = {
    val prefix = tfName.split("[\\(/]").toList
    p.internal.getProducingTransformInternal.getFullName
      .split("[\\(/]")
      .toList
      .take(prefix.length) shouldBe prefix
  }

}
