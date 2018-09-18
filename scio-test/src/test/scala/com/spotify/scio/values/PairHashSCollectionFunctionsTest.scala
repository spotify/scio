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

package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec

class PairHashSCollectionFunctionsTest extends PipelineSpec {

  it should "support hashJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.hashJoin(p2)
      p should containInAnyOrder(Seq(("a", (1, 11)), ("b", (2, 12))))
    }
  }

  it should "support hashJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.hashJoin(p2)
      p should
        containInAnyOrder(Seq(("a", (1, 11)), ("a", (2, 11)), ("b", (3, 12)), ("b", (3, 13))))
    }
  }

  it should "support hashJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq.empty[(String, Int)])
      val p = p1.hashJoin(p2)
      p should haveSize(0)
    }
  }

  it should "support hashLeftJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.hashLeftJoin(p2)
      p should containInAnyOrder(Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support hashLeftJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq.empty[(String, Int)])
      val p = p1.hashLeftJoin(p2)
      val empty = Option.empty[Int]
      p should containInAnyOrder(Seq(("a", (1, empty)), ("b", (2, empty)), ("c", (3, empty))))
    }
  }

  it should "support hashLeftJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.hashLeftJoin(p2)
      p should containInAnyOrder(
        Seq(("a", (1, Some(11))),
            ("a", (2, Some(11))),
            ("b", (3, Some(12))),
            ("b", (3, Some(13))),
            ("c", (4, None))))
    }
  }

  it should "support hashFullOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2)))
      val p2 = sc.parallelize(Seq(("a", 11), ("c", 13)))
      val p = p1.hashFullOuterJoin(p2)
      p should containInAnyOrder(
        Seq(("a", (Some(1), Some(11))), ("b", (Some(2), None)), ("c", (None, Some(13)))))
    }
  }

  it should "support hashFullOuterJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2)))
      val p2 = sc.parallelize(Seq.empty[(String, Int)])
      val p = p1.hashFullOuterJoin(p2)
      val empty = Option.empty[Int]
      val some = Option[Int] _
      p should containInAnyOrder(Seq(("a", (some(1), empty)), ("b", (some(2), empty))))
    }
  }

  it should "support hashFullOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13), ("d", 14)))
      val p = p1.hashFullOuterJoin(p2)
      p should containInAnyOrder(
        Seq(("a", (Some(1), Some(11))),
            ("a", (Some(2), Some(11))),
            ("b", (Some(3), Some(12))),
            ("b", (Some(3), Some(13))),
            ("c", (Some(4), None)),
            ("d", (None, Some(14)))))
    }
  }
}
