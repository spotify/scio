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

import com.twitter.algebird.{Aggregator, Semigroup}
import com.spotify.scio.coders.Coder
import org.joda.time.Instant

class SCollectionWithFanoutTest extends NamedTransformSpec {
  "SCollectionWithFanout" should "support aggregate()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100).withFanout(10)
      val p1 = p.aggregate(0.0)(_ + _, _ + _)
      val p2 = p.aggregate(Aggregator.max[Int])
      val p3 = p.aggregate(Aggregator.immutableSortedReverseTake[Int](5))
      p1 should containSingleValue(5050.0)
      p2 should containSingleValue(100)
      p3 should containSingleValue(Seq(100, 99, 98, 97, 96))
    }
  }

  it should "support combine()" in {
    runWithContext { sc =>
      val p = sc
        .parallelize(1 to 100)
        .withFanout(10)
        .combine(_.toDouble)(_ + _)(_ + _)
      p should containSingleValue(5050.0)
    }
  }

  it should "support fold()" in {
    runWithContext { sc =>
      val p = sc.parallelize(1 to 100).withFanout(10)
      val r1 = p.fold(0)(_ + _)
      val r2 = p.fold
      r1 should containSingleValue(5050)
      r2 should containSingleValue(5050)
    }
  }

  it should "support reduce()" in {
    runWithContext { sc =>
      val p = sc.parallelize(Seq(1, 2, 3, 4, 5)).withFanout(10).reduce(_ + _)
      p should containSingleValue(15)
    }
  }

  it should "support min" in {
    runWithContext { sc =>
      def min[T: Coder: Ordering](elems: T*): SCollection[T] =
        sc.parallelize(elems).withFanout(10).min
      min(1, 2, 3) should containSingleValue(1)
      min(1L, 2L, 3L) should containSingleValue(1L)
      min(1f, 2f, 3f) should containSingleValue(1f)
      min(1.0, 2.0, 3.0) should containSingleValue(1.0)
      min(1 to 100: _*) should containSingleValue(1)
    }
  }

  it should "support max" in {
    runWithContext { sc =>
      def max[T: Coder: Ordering](elems: T*): SCollection[T] =
        sc.parallelize(elems).withFanout(10).max
      max(1, 2, 3) should containSingleValue(3)
      max(1L, 2L, 3L) should containSingleValue(3L)
      max(1f, 2f, 3f) should containSingleValue(3f)
      max(1.0, 2.0, 3.0) should containSingleValue(3.0)
      max(1 to 100: _*) should containSingleValue(100)
    }
  }

  it should "support latest" in {
    runWithContext { sc =>
      def latest(elems: Long*): SCollection[Long] =
        sc.parallelize(elems).timestampBy(Instant.ofEpochMilli).withFanout(10).latest
      latest(1L, 2L, 3L) should containSingleValue(3L)
      latest(1L to 100L: _*) should containSingleValue(100L)
    }
  }

  it should "support sum" in {
    runWithContext { sc =>
      def sum[T: Coder: Semigroup](elems: T*): SCollection[T] =
        sc.parallelize(elems).withFanout(10).sum
      sum(1, 2, 3) should containSingleValue(6)
      sum(1L, 2L, 3L) should containSingleValue(6L)
      sum(1f, 2f, 3f) should containSingleValue(6f)
      sum(1.0, 2.0, 3.0) should containSingleValue(6.0)
      sum(1 to 100: _*) should containSingleValue(5050)
    }
  }

  it should "support mean" in {
    runWithContext { sc =>
      def mean[T: Coder: Numeric](elems: T*): SCollection[Double] =
        sc.parallelize(elems).withFanout(10).mean
      mean(1, 2, 3) should containSingleValue(2.0)
      mean(1L, 2L, 3L) should containSingleValue(2.0)
      mean(1f, 2f, 3f) should containSingleValue(2.0)
      mean(1.0, 2.0, 3.0) should containSingleValue(2.0)
      mean(0 to 100: _*) should containSingleValue(50.0)
    }
  }

  it should "support top()" in {
    runWithContext { sc =>
      def top3[T: Ordering: Coder](elems: T*): SCollection[Iterable[T]] =
        sc.parallelize(elems).withFanout(10).top(3)

      top3(1, 2, 3, 4) should containSingleValue(Iterable(4, 3, 2))
      top3(1L, 2L, 3L, 4L) should containSingleValue(Iterable(4L, 3L, 2L))
      top3(1f, 2f, 3f, 4f) should containSingleValue(Iterable(4f, 3f, 2f))
      top3(1.0, 2.0, 3.0, 4.0) should containSingleValue(Iterable(4.0, 3.0, 2.0))
    }
  }

  private def shouldFanOut[T](fn: SCollectionWithFanout[Int] => SCollection[T]) = {
    runWithContext { sc =>
      val p = fn(sc.parallelize(1 to 100).withFanout(10))
      assertGraphContainsStepRegex(p, "Combine\\.perKeyWithFanout\\([^)]*\\)")
    }
  }

  it should "fan out with aggregate(zeroValue)(seqOp)" in {
    shouldFanOut(_.aggregate(0.0)(_ + _, _ + _))
  }

  it should "fan out with aggregate(Aggregator)" in {
    shouldFanOut(_.aggregate(Aggregator.max[Int]))
  }

  it should "fan out with aggregate(MonoidAggregator)" in {
    shouldFanOut(_.aggregate(Aggregator.immutableSortedReverseTake[Int](5)))
  }

  it should "fan out with combine()" in {
    shouldFanOut(_.combine(_.toDouble)(_ + _)(_ + _))
  }

  it should "fan out with fold(zeroValue)(op)" in {
    shouldFanOut(_.fold(0)(_ + _))
  }

  it should "fan out with fold(Monoid)" in {
    shouldFanOut(_.fold)
  }

  it should "fan out with reduce()" in {
    shouldFanOut(_.reduce(_ + _))
  }

  it should "fan out with sum()" in {
    shouldFanOut(_.sum)
  }
}
