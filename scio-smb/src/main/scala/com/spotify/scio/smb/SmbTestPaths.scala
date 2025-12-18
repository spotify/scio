/*
 * Copyright 2025 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.smb

import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._

/**
 * Registry for mapping logical test paths to physical temp directories containing SMB files.
 *
 * This enables JobTest to work with SMBCollection by writing test data as real SMB files to temp
 * directories and resolving logical paths (like "gs://users") to these temp paths.
 *
 * Internal API - not intended for direct use by end users.
 */
private[scio] object SmbTestPaths {
  private val paths = new ConcurrentHashMap[String, String]()

  /**
   * Register a mapping from test ID to physical path.
   *
   * @param testId
   *   Test identifier (typically from SmbIO.testId)
   * @param path
   *   Physical path to temp directory containing SMB files
   */
  def register(testId: String, path: String): Unit = {
    paths.put(testId, path)
    ()
  }

  /**
   * Resolve a test ID to its physical path.
   *
   * @param testId
   *   Test identifier
   * @return
   *   Some(path) if registered, None otherwise
   */
  def resolve(testId: String): Option[String] =
    Option(paths.get(testId))

  /**
   * Get all registered paths (for cleanup).
   *
   * @return
   *   Map of testId -> path
   */
  def getAll: Map[String, String] =
    paths.asScala.toMap

  /**
   * Clear all registered paths.
   *
   * IMPORTANT: Call this in test teardown to prevent cross-test pollution:
   * {{{
   * class MyTest extends PipelineSpec {
   *   override def afterAll(): Unit = {
   *     SmbTestPaths.clear()
   *     super.afterAll()
   *   }
   * }
   * }}}
   */
  def clear(): Unit =
    paths.clear()
}
