/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.smb

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ExhaustableLazyIterableTest extends AnyFlatSpec with Matchers {

  "ExhaustableLazyIterable" should "allow single iteration" in {
    val iter = Iterator(1, 2, 3)
    val iterable = new ExhaustableLazyIterable(iter)

    val result = iterable.toList
    result shouldBe List(1, 2, 3)
  }

  it should "fail on second iteration" in {
    val iter = Iterator(1, 2, 3)
    val iterable = new ExhaustableLazyIterable(iter)

    iterable.iterator.toList // First iteration - OK

    val ex = intercept[IllegalStateException] {
      iterable.iterator.toList // Second iteration - FAIL
    }

    ex.getMessage should include("SMBCollection values are lazy iterables")
    ex.getMessage should include("can only be consumed once")
    ex.getMessage should include("toSeq")
  }

  it should "exhaust unconsumed iterator" in {
    val iter = Iterator(1, 2, 3)
    val iterable = new ExhaustableLazyIterable(iter)

    // Don't consume
    iterable.isStarted shouldBe false
    iterable.isExhausted shouldBe false

    // Exhaust
    iterable.exhaust()

    iterable.isStarted shouldBe false // Never called iterator()
    iterable.isExhausted shouldBe true
  }

  it should "exhaust partially consumed iterator" in {
    val iter = Iterator(1, 2, 3, 4, 5)
    val iterable = new ExhaustableLazyIterable(iter)

    // Consume first 2 elements
    val it = iterable.iterator
    it.next() shouldBe 1
    it.next() shouldBe 2

    iterable.isStarted shouldBe true
    iterable.isExhausted shouldBe false

    // Exhaust remaining
    iterable.exhaust()

    iterable.isExhausted shouldBe true
  }

  it should "handle materialization to Seq" in {
    val iter = Iterator(1, 2, 3)
    val iterable = new ExhaustableLazyIterable(iter)

    val seq = iterable.toSeq

    // Can iterate Seq multiple times
    seq.sum shouldBe 6
    seq.max shouldBe 3
    seq.size shouldBe 3
  }

  it should "support single-pass operations without materialization" in {
    val iter = Iterator(1, 2, 3, 4, 5)
    val iterable = new ExhaustableLazyIterable(iter)

    // Single-pass fold - no materialization
    val sum = iterable.foldLeft(0)(_ + _)
    sum shouldBe 15

    // Iterator should be exhausted
    iterable.isExhausted shouldBe true
  }

  it should "exhaust empty iterator safely" in {
    val iter: Iterator[Int] = Iterator.empty
    val iterable = new ExhaustableLazyIterable(iter)

    iterable.isExhausted shouldBe true
    iterable.exhaust() // Should not throw
    iterable.isExhausted shouldBe true
  }

  it should "exhaust fully consumed iterator safely" in {
    val iter = Iterator(1, 2, 3)
    val iterable = new ExhaustableLazyIterable(iter)

    // Fully consume
    iterable.iterator.toList

    iterable.isExhausted shouldBe true
    iterable.exhaust() // Should not throw (already exhausted)
    iterable.isExhausted shouldBe true
  }

  it should "work with map and filter (lazy transformations)" in {
    val iter = Iterator(1, 2, 3, 4, 5)
    val iterable = new ExhaustableLazyIterable(iter)

    // Lazy transformations
    val result = iterable
      .filter(_ % 2 == 0)
      .map(_ * 2)
      .toList

    result shouldBe List(4, 8)
  }

  it should "have meaningful toString representations for different states" in {
    // Test pending state
    val iter1 = Iterator(1, 2, 3)
    val iterable1 = new ExhaustableLazyIterable(iter1)
    iterable1.toString should include("pending")

    // Test in-progress state
    val iter2 = Iterator(1, 2, 3)
    val iterable2 = new ExhaustableLazyIterable(iter2)
    val it = iterable2.iterator
    it.next() // Consume one element
    iterable2.toString should include("in-progress")

    // Test exhausted state
    val iter3 = Iterator(1, 2, 3)
    val iterable3 = new ExhaustableLazyIterable(iter3)
    iterable3.iterator.toList // Fully consume
    iterable3.toString should include("exhausted")
  }
}
