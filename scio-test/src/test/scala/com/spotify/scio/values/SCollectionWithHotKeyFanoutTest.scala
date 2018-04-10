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
import com.twitter.algebird.Aggregator

class SCollectionWithHotKeyFanoutTest extends PipelineSpec {

  "SCollectionWithHotKeyFanout" should "support aggregateByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100).map(("a", _)) ++ sc.parallelize(1 to 10).map(("b", _))
      val pa = p.withHotKeyFanout(10)
      val pb = p.withHotKeyFanout(_.hashCode)
      val r1a = pa.aggregateByKey(0.0)(_ + _, _ + _)
      val r1b = pb.aggregateByKey(0.0)(_ + _, _ + _)
      val r2a = pa.aggregateByKey(Aggregator.max[Int])
      val r2b = pb.aggregateByKey(Aggregator.max[Int])
      val r3a = pa.aggregateByKey(Aggregator.immutableSortedReverseTake[Int](5))
      val r3b = pb.aggregateByKey(Aggregator.immutableSortedReverseTake[Int](5))
      r1a should containInAnyOrder (Seq(("a", 5050.0), ("b", 55.0)))
      r1b should containInAnyOrder (Seq(("a", 5050.0), ("b", 55.0)))
      r2a should containInAnyOrder (Seq(("a", 100), ("b", 10)))
      r2b should containInAnyOrder (Seq(("a", 100), ("b", 10)))
      val r3expected = Seq(("a", Seq(100, 99, 98, 97, 96)), ("b", Seq(10, 9, 8, 7, 6)))
      r3a should containInAnyOrder (r3expected)
      r3b should containInAnyOrder (r3expected)
    }
  }

  it should "support combineByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val r1 = (p1 ++ p2).withHotKeyFanout(10).combineByKey(_.toDouble)(_ + _)(_ + _)
      val r2 = (p1 ++ p2).withHotKeyFanout(_.hashCode).combineByKey(_.toDouble)(_ + _)(_ + _)
      r1 should containInAnyOrder (Seq(("a", 5050.0), ("b", 55.0)))
      r2 should containInAnyOrder (Seq(("a", 5050.0), ("b", 55.0)))
    }
  }

  it should "support foldByKey()" in {
    runWithContext { sc =>
      val p1 = sc.parallelize(1 to 100).map(("a", _))
      val p2 = sc.parallelize(1 to 10).map(("b", _))
      val r1 = (p1 ++ p2).withHotKeyFanout(10).foldByKey(0)(_ + _)
      val r2 = (p1 ++ p2).withHotKeyFanout(_.hashCode).foldByKey(0)(_ + _)
      val r3 = (p1 ++ p2).withHotKeyFanout(10).foldByKey
      val r4 = (p1 ++ p2).withHotKeyFanout(_.hashCode).foldByKey
      r1 should containInAnyOrder (Seq(("a", 5050), ("b", 55)))
      r2 should containInAnyOrder (Seq(("a", 5050), ("b", 55)))
      r3 should containInAnyOrder (Seq(("a", 5050), ("b", 55)))
      r4 should containInAnyOrder (Seq(("a", 5050), ("b", 55)))
    }
  }

  it should "support reduceByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(("a", 1), ("b", 1), ("b", 2), ("c", 1), ("c", 2), ("c", 3)))
      val r1 = p.withHotKeyFanout(10).reduceByKey(_ + _)
      val r2 = p.withHotKeyFanout(_.hashCode).reduceByKey(_ + _)
      r1 should containInAnyOrder (Seq(("a", 1), ("b", 3), ("c", 6)))
      r2 should containInAnyOrder (Seq(("a", 1), ("b", 3), ("c", 6)))
    }
  }

  it should "support sumByKey()" in {
    runWithContext { sc =>
      val p = sc.parallelize(List(("a", 1), ("b", 2), ("b", 2)) ++ (1 to 100).map(("c", _)))
      val r1 = p.withHotKeyFanout(10).sumByKey
      val r2 = p.withHotKeyFanout(_.hashCode).sumByKey
      r1 should containInAnyOrder (Seq(("a", 1), ("b", 4), ("c", 5050)))
      r2 should containInAnyOrder (Seq(("a", 1), ("b", 4), ("c", 5050)))
    }
  }


}
