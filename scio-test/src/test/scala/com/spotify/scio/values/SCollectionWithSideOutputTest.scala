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

import com.spotify.scio.ScioContext

class SCollectionWithSideOutputTest extends NamedTransformSpec {
  "SCollectionWithSideOutput" should "support custom transform name set before application" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(Seq("a", "b", "c"))
    val p2 = SideOutput[String]()
    val (main, _) = p1
      .withName("MapWithSideOutputs")
      .withSideOutputs(p2)
      .map { (x, s) => s.output(p2, x + "2"); x + "1" }

    assertTransformNameStartsWith(main, "MapWithSideOutputs")
  }

  it should "support custom transform name set after application" in {
    val sc = ScioContext()
    val p1 = sc.parallelize(Seq("a", "b", "c"))
    val p2 = SideOutput[String]()
    val (main, _) = p1
      .withSideOutputs(p2)
      .withName("MapWithSideOutputs")
      .map { (x, s) => s.output(p2, x + "2"); x + "1" }

    assertTransformNameStartsWith(main, "MapWithSideOutputs")
  }

  it should "support map()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2).map { (x, s) => s.output(p2, x + "2"); x + "1" }
      main should containInAnyOrder(Seq("a1", "b1", "c1"))
      side(p2) should containInAnyOrder(Seq("a2", "b2", "c2"))
    }
  }

  it should "support flatMap()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c"))
      val p2 = SideOutput[String]()
      val (main, side) = p1.withSideOutputs(p2).flatMap { (x, s) =>
        s.output(p2, x + "2x").output(p2, x + "2y")
        Seq(x + "1x", x + "1y")
      }
      main should containInAnyOrder(Seq("a1x", "a1y", "b1x", "b1y", "c1x", "c1y"))
      side(p2) should containInAnyOrder(Seq("a2x", "a2y", "b2x", "b2y", "c2x", "c2y"))
    }
  }

  it should "not break when a side output is not applied (#1587)" in runWithContext { sc =>
    val nSideOut = SideOutput[Int]()
    val expected = List(2, 4, 6, 8, 10)
    val elements = sc.parallelize(1 to 10)
    val (even, _) =
      elements
        .withSideOutputs(nSideOut)
        .flatMap { case (n, ctx) =>
          if (n % 2 == 0)
            Some(n)
          else {
            ctx.output(nSideOut, n)
            None
          }
        }

    even should containInAnyOrder(expected)
  }
}
