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

import com.sun.management.OperatingSystemMXBean
import java.lang.management.ManagementFactory
import java.util.concurrent.atomic.AtomicLong
import org.slf4j.LoggerFactory

/**
 * Tracks memory budgets for sparkey readers on this JVM, covering both off-heap and on-heap memory.
 * On Dataflow, the JVM heap is hardcoded to ~70% of worker memory, leaving only ~30% for off-heap
 * use (page cache, OS, kernel, etc.).
 *
 * Sparkey readers are opened via mmap (off-heap) by default. When the off-heap budget is exhausted,
 * readers can fall back to heap-backed mode. This tracker provides atomic budget claiming for both
 * pools to coordinate across threads.
 *
 * Budget is claimed but never released — readers are cached for the JVM lifetime and never closed.
 * If a reader fails to open partway through (e.g. a sharded sparkey with some shards already
 * claimed), the budget for those shards is leaked. This is acceptable since a reader failure is
 * fatal to the pipeline.
 */
private[sparkey] class HostMemoryTracker(val offHeapBudget: Long, val heapBudget: Long) {

  private val remainingOffHeap = new AtomicLong(offHeapBudget)
  private val remainingHeap = new AtomicLong(heapBudget)
  private val totalUnboundedOffHeap = new AtomicLong(0)

  /**
   * Atomically try to claim `bytes` from the off-heap budget. Returns true if the claim succeeded
   * (enough budget remaining), false otherwise.
   */
  def tryClaimOffHeap(bytes: Long): Boolean = tryClaim(remainingOffHeap, bytes)

  /**
   * Atomically try to claim `bytes` from the heap budget. Returns true if the claim succeeded
   * (enough budget remaining), false otherwise.
   */
  def tryClaimHeap(bytes: Long): Boolean = tryClaim(remainingHeap, bytes)

  /** Record `bytes` of unbounded off-heap mmap usage (neither budget had room). */
  def addUnboundedOffHeap(bytes: Long): Long = totalUnboundedOffHeap.addAndGet(bytes)

  private def tryClaim(budget: AtomicLong, bytes: Long): Boolean = {
    val prev =
      budget.getAndAccumulate(bytes, (current, b) => if (current >= b) current - b else current)
    prev >= bytes
  }
}

private[sparkey] object HostMemoryTracker {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // Reserve 2 GB of off-heap memory for OS, kernel structures, JVM class files, Beam shuffle, etc.
  private val OffHeapReserveBytes: Long = 2L * 1024 * 1024 * 1024

  // Reserve 10% of max heap (min 4 GB) for GC headroom and non-sparkey allocations.
  private val HeapReserveBytes: Long = {
    val maxHeap = Runtime.getRuntime.maxMemory()
    Math.max(4L * 1024 * 1024 * 1024, (maxHeap * 0.1).toLong)
  }

  private val offHeapBudget: Long = {
    val totalPhysical = ManagementFactory.getOperatingSystemMXBean
      .asInstanceOf[OperatingSystemMXBean]
      .getTotalPhysicalMemorySize
    val maxHeap = Runtime.getRuntime.maxMemory()
    Math.max(0, totalPhysical - maxHeap - OffHeapReserveBytes)
  }

  private val heapBudget: Long = {
    val maxHeap = Runtime.getRuntime.maxMemory()
    Math.max(0, maxHeap - HeapReserveBytes)
  }

  val instance: HostMemoryTracker = new HostMemoryTracker(offHeapBudget, heapBudget)

  logger.info(
    "HostMemoryTracker — off-heap budget: {} bytes, heap budget: {} bytes",
    Array[AnyRef](Long.box(offHeapBudget), Long.box(heapBudget)): _*
  )
}
