/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.extra.sparkey

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class HostMemoryTrackerTest extends AnyFlatSpec with Matchers {

  "HostMemoryTracker" should "claim off-heap budget when sufficient" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 100, heapBudget = 50)
    tracker.tryClaimOffHeap(60) shouldBe true
    tracker.tryClaimOffHeap(40) shouldBe true
  }

  it should "reject off-heap claim when insufficient" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 100, heapBudget = 50)
    tracker.tryClaimOffHeap(60) shouldBe true
    tracker.tryClaimOffHeap(50) shouldBe false
  }

  it should "claim heap budget when sufficient" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 0, heapBudget = 100)
    tracker.tryClaimHeap(60) shouldBe true
    tracker.tryClaimHeap(40) shouldBe true
  }

  it should "reject heap claim when insufficient" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 0, heapBudget = 100)
    tracker.tryClaimHeap(60) shouldBe true
    tracker.tryClaimHeap(50) shouldBe false
  }

  it should "handle zero budgets" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 0, heapBudget = 0)
    tracker.tryClaimOffHeap(1) shouldBe false
    tracker.tryClaimHeap(1) shouldBe false
  }

  it should "handle exact budget claims" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 100, heapBudget = 50)
    tracker.tryClaimOffHeap(100) shouldBe true
    tracker.tryClaimOffHeap(1) shouldBe false
  }

  it should "track off-heap and heap budgets independently" in {
    val tracker = new HostMemoryTracker(offHeapBudget = 100, heapBudget = 100)
    tracker.tryClaimOffHeap(100) shouldBe true
    tracker.tryClaimOffHeap(1) shouldBe false
    // heap should still be available
    tracker.tryClaimHeap(100) shouldBe true
    tracker.tryClaimHeap(1) shouldBe false
  }

  "HostMemoryTracker.instance" should "exist as a singleton" in {
    HostMemoryTracker.instance should not be null
    HostMemoryTracker.instance shouldBe theSameInstanceAs(HostMemoryTracker.instance)
  }
}
