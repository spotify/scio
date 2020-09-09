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

import com.spotify.scio.options.ScioOptions
import com.spotify.scio.testing.PipelineSpec

class PairHashSCollectionFunctionsTest extends PipelineSpec {
  "PairSCollection" should "support hashJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.hashJoin(p2)
      p should containInAnyOrder(Seq(("a", (1, 11)), ("b", (2, 12))))
    }
  }

  it should "support hashJoin() with nulls" in {
    runWithContext { sc =>
      sc.optionsAs[ScioOptions].setNullableCoders(true)

      val p1 = sc.parallelize(Seq((null, "1"), (null, "2"), ("b", "3")))
      val p2 = sc.parallelize(Seq((null, "11"), ("b", null), ("b", "13")))
      val p = p1.hashJoin(p2)
      p should
        containInAnyOrder(
          Seq((null, ("1", "11")), (null, ("2", "11")), ("b", ("3", null)), ("b", ("3", "13")))
        )
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

  it should "support hashJoin() with .asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13))).asMultiMapSideInput
      val p = p1.hashJoin(p2)
      p should
        containInAnyOrder(Seq(("a", (1, 11)), ("a", (2, 11)), ("b", (3, 12)), ("b", (3, 13))))
    }
  }

  it should "support hashJoin() with empty .asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3)))
      val p2 = sc.parallelize[(String, Int)](Map.empty).asMultiMapSideInput
      val p = p1.hashJoin(p2)
      p should
        containInAnyOrder(Seq.empty[(String, (Int, Int))])
    }
  }

  it should "support hashLeftOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14)))
      val p = p1.hashLeftOuterJoin(p2)
      p should containInAnyOrder(Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support hashLeftOuterJoin() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq.empty[(String, Int)])
      val p = p1.hashLeftOuterJoin(p2)
      val empty = Option.empty[Int]
      p should containInAnyOrder(Seq(("a", (1, empty)), ("b", (2, empty)), ("c", (3, empty))))
    }
  }

  it should "support hashLeftOuterJoin() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("a", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("b", 13)))
      val p = p1.hashLeftOuterJoin(p2)
      p should containInAnyOrder(
        Seq(
          ("a", (1, Some(11))),
          ("a", (2, Some(11))),
          ("b", (3, Some(12))),
          ("b", (3, Some(13))),
          ("c", (4, None))
        )
      )
    }
  }

  it should "support hashLeftOuterJoin() with asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq(("a", 11), ("b", 12), ("d", 14))).asMultiMapSideInput
      val p = p1.hashLeftOuterJoin(p2)
      p should containInAnyOrder(Seq(("a", (1, Some(11))), ("b", (2, Some(12))), ("c", (3, None))))
    }
  }

  it should "support hashFullOuterJoin()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2)))
      val p2 = sc.parallelize(Seq(("a", 11), ("c", 13)))
      val p = p1.hashFullOuterJoin(p2)
      p should containInAnyOrder(
        Seq(("a", (Some(1), Some(11))), ("b", (Some(2), None)), ("c", (None, Some(13))))
      )
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
        Seq(
          ("a", (Some(1), Some(11))),
          ("a", (Some(2), Some(11))),
          ("b", (Some(3), Some(12))),
          ("b", (Some(3), Some(13))),
          ("c", (Some(4), None)),
          ("d", (None, Some(14)))
        )
      )
    }
  }

  it should "support hashFullOuterJoin() with no overlap" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1)))
      val p2 = sc.parallelize(Seq(("b", 2)))
      val p = p1.hashFullOuterJoin(p2)
      p should containInAnyOrder(Seq(("a", (Some(1), None)), ("b", (None, Some(2)))))
    }
  }

  it should "support hashFullOuterJoin() with asMultiMapSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2)))
      val p2 = sc.parallelize(Seq(("a", 11), ("c", 13))).asMultiMapSideInput
      val p = p1.hashFullOuterJoin(p2)
      p should containInAnyOrder(
        Seq(("a", (Some(1), Some(11))), ("b", (Some(2), None)), ("c", (None, Some(13))))
      )
    }
  }

  it should "support hashIntersectByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.hashIntersectByKey(p2)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support hashIntersectByKey() with duplicate keys" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq("a", "b", "b", "d"))
      val p = p1.hashIntersectByKey(p2)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2), ("b", 4)))
    }
  }

  it should "support hashIntersectByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq[(String, Unit)]())
      val p2 = sc.parallelize(Seq("a", "b", "d"))
      val p = p1.hashIntersectByKey(p2)
      p should beEmpty
    }
  }

  it should "support hashIntersectByKey() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq[String]())
      val p = p1.hashIntersectByKey(p2)
      p should beEmpty
    }
  }

  it should "support hashIntersectByKey() with asSetSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4)))
      val p2 = sc.parallelize(Seq[String]("a", "b", "d")).asSetSingletonSideInput
      val p = p1.hashIntersectByKey(p2)
      p should containInAnyOrder(Seq(("a", 1), ("b", 2), ("b", 4)))
    }
  }

  it should "support hashSubtractByKey() with empty RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2)))
      val p2 = sc.parallelize(Seq.empty[String])
      val output = p1.hashSubtractByKey(p2)
      output should haveSize(2)
      output should containInAnyOrder(Seq(("a", 1), ("b", 2)))
    }
  }

  it should "support hashSubtractByKey() with empty LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq.empty[(String, Any)])
      val p2 = sc.parallelize(Seq("1", "2", "3"))
      val output = p1.hashSubtractByKey(p2)
      output should beEmpty
    }
  }

  it should "support hashSubtractByKey() with non-empty RHS / LHS and no duplicates" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("d", 3)))
      val p2 = sc.parallelize(Seq("a", "d"))
      val output = p1.hashSubtractByKey(p2)
      output should haveSize(2)
      output should containInAnyOrder(Seq(("b", 2), ("c", 3)))
    }
  }

  it should "support hashSubtractByKey() with duplicate keys in LHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3), ("b", 4), ("d", 5)))
      val p2 = sc.parallelize(Seq("a", "c"))
      val output = p1.hashSubtractByKey(p2)
      output should haveSize(3)
      output should containInAnyOrder(Seq(("b", 2), ("b", 4), ("d", 5)))
    }
  }

  it should "support hashSubtractByKey() with set singleton side input RHS" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("b", 3), ("c", 4)))
      val p2 = sc.parallelize(Seq[String]("a", "b")).asSetSingletonSideInput
      val output = p1.hashSubtractByKey(p2)
      output should haveSize(1)
      output should containInAnyOrder(Seq(("c", 4)))
    }
  }

  it should "support hashFilter() with asSetSingletonSideInput" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(Seq("a", "b", "c", "b"))
      val p2 = sc.parallelize(Seq[String]("a", "a", "b", "e")).asSetSingletonSideInput
      val p = p1.hashFilter(p2)
      p should containInAnyOrder(Seq("a", "b", "b"))
    }
  }
}
