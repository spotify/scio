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

package com.spotify.scio.parquet

import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.{ProcessElement, Setup}
import org.slf4j.LoggerFactory

/**
 * DoFn that reports directory-level source lineage for legacy Parquet reads.
 *
 * @param filePattern
 *   The file pattern or path to report lineage for
 */
private[parquet] class LineageReportingDoFn[T](filePattern: String) extends DoFn[T, T] {

  @transient
  private lazy val logger = LoggerFactory.getLogger(classOf[LineageReportingDoFn[T]])

  @Setup
  def setup(): Unit = {
    try {
      val isDirectory = filePattern.endsWith("/")
      val resourceId = FileSystems.matchNewResource(filePattern, isDirectory)
      val directory = resourceId.getCurrentDirectory
      FileSystems.reportSourceLineage(directory)
    } catch {
      case e: Exception =>
        logger.warn(
          s"Error when reporting lineage for pattern: $filePattern",
          e
        )
    }
  }

  @ProcessElement
  def processElement(@DoFn.Element element: T, out: DoFn.OutputReceiver[T]): Unit =
    out.output(element)
}
