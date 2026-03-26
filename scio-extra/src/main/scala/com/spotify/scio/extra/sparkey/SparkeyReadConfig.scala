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

import com.spotify.sparkey.LoadMode

/**
 * Configuration for reading Sparkey side inputs.
 *
 * @param loadMode
 *   page cache prefetch mode for mmap-backed shards. Ignored for heap-backed shards.
 * @param heapBudgetBytes
 *   maximum bytes to read into JVM heap across all shards in this side input. Shards are loaded
 *   largest-first to maximize heap utilization. Shards that don't fit within the remaining budget
 *   fall back to memory-mapped files. Default is 0 (all mmap, matching current behavior).
 */
case class SparkeyReadConfig(
  loadMode: LoadMode = LoadMode.NONE,
  heapBudgetBytes: Long = 0
)

object SparkeyReadConfig {
  val Default: SparkeyReadConfig = SparkeyReadConfig()
}
