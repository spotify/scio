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

import com.spotify.sparkey.Sparkey
import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files

class OpenWithMemoryTrackingTest extends AnyFlatSpec with Matchers {

  private def withSparkey[T](test: File => T): T = {
    val dir = Files.createTempDirectory("sparkey-test").toFile
    try {
      val base = new File(dir, "test")
      val writer = Sparkey.createNew(base)
      writer.put("key1", "value1")
      writer.put("key2", "value2")
      writer.flush()
      writer.writeHash()
      writer.close()
      test(base)
    } finally {
      FileUtils.deleteDirectory(dir)
    }
  }

  "openWithMemoryTracking" should "open via mmap when off-heap budget is available" in {
    withSparkey { base =>
      val tracker = new HostMemoryTracker(offHeapBudget = Long.MaxValue, heapBudget = 0)
      val (reader, plans) = ShardedSparkeyUri.openWithMemoryTracking(base, tracker)
      try {
        reader.getAsString("key1") shouldBe "value1"
        plans should have size 1
        plans.head.useHeap shouldBe false
        plans.head.budgeted shouldBe true
      } finally {
        reader.close()
      }
    }
  }

  it should "open on heap when off-heap is exhausted but heap is available" in {
    withSparkey { base =>
      val tracker = new HostMemoryTracker(offHeapBudget = 0, heapBudget = Long.MaxValue)
      val (reader, plans) = ShardedSparkeyUri.openWithMemoryTracking(base, tracker)
      try {
        reader.getAsString("key1") shouldBe "value1"
        plans should have size 1
        plans.head.useHeap shouldBe true
        plans.head.budgeted shouldBe true
      } finally {
        reader.close()
      }
    }
  }

  it should "fall back to unbounded mmap when neither budget is available" in {
    withSparkey { base =>
      val tracker = new HostMemoryTracker(offHeapBudget = 0, heapBudget = 0)
      val (reader, plans) = ShardedSparkeyUri.openWithMemoryTracking(base, tracker)
      try {
        reader.getAsString("key1") shouldBe "value1"
        plans should have size 1
        plans.head.useHeap shouldBe false
        plans.head.budgeted shouldBe false
      } finally {
        reader.close()
      }
    }
  }
}
