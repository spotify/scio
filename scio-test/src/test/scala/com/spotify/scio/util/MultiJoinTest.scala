/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.util

import com.spotify.scio.testing.PipelineSpec

class MultiJoinTest extends PipelineSpec {

  import com.spotify.scio.testing.TestingUtils._

  "MultiJoin" should "support cogroup()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("d", 14L)))
      val r = MultiJoin.cogroup(p1, p2)
      val expected = Seq(
        ("a", (iterable(1), iterable(11L))),
        ("b", (iterable(2), iterable(12L))),
        ("c", (iterable(3), iterable())),
        ("d", (iterable(), iterable(14L))))
      r should containInAnyOrder (expected)
    }
  }

  it should "support cogroup() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11L), ("b", 12L), ("b", 13L), ("d", 14L)))
      val fn =
        (t: (String, (Iterable[Int], Iterable[Long]))) => (t._1, (t._2._1.toSet, t._2._2.toSet))
      val r = MultiJoin.cogroup(p1, p2).map(fn)
      val expected = Seq[(String, (Set[Int], Set[Long]))](
        ("a", (Set(1, 2), Set(11L))),
        ("b", (Set(2), Set(12L, 13L))),
        ("c", (Set(3), Set())),
        ("d", (Set(), Set(14L))))
      r should containInAnyOrder (expected)
    }
  }

  it should "support fullOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = MultiJoin.outer(p1, p2)
      p should containInAnyOrder (Seq(
        ("a", (Some(1), Some(11))),
        ("b", (Some(2), Some(12))),
        ("c", (Some(3), None)),
        ("d", (None, Some(14)))))
    }
  }

  it should "support fullOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = MultiJoin.outer(p1, p2)
      p should containInAnyOrder (Seq(
        ("a", (Some(1), Some(11))),
        ("a", (Some(2), Some(11))),
        ("b", (Some(3), Some(12))),
        ("b", (Some(3), Some(13))),
        ("c", (Some(4), None)),
        ("d", (None, Some(14)))))
    }
  }

  it should "support join()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = MultiJoin(p1, p2)
      p should containInAnyOrder (Seq(("a", (1, 11)), ("b", (2, 12))))
    }
  }

  it should "support leftOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = MultiJoin.left(p1, p2)
      p should containInAnyOrder (Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support leftOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = MultiJoin.left(p1, p2)
      p should containInAnyOrder (Seq(
        ("a", (1, Some(11))),
        ("a", (2, Some(11))),
        ("b", (3, Some(12))),
        ("b", (3, Some(13))),
        ("c", (4, None))))
    }
  }

}
