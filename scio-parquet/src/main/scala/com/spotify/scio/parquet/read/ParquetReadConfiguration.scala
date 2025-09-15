/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.parquet.read

import org.apache.beam.sdk.options.{ExperimentalOptions, PipelineOptions}
import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory

object ParquetReadConfiguration {
  private val log = LoggerFactory.getLogger(getClass)

  // Key
  val SplitGranularity = "scio.parquet.read.splitgranularity"

  // Values
  val SplitGranularityFile = "file"
  val SplitGranularityRowGroup = "rowgroup"

  // HadoopFormatIO
  @deprecated(
    "scio.parquet.read.typed.skipClone is deprecated and will be removed once HadoopFormatIO support is dropped " +
      "in Parquet reads.",
    since = "0.12.0"
  )
  val SkipClone = "scio.parquet.read.typed.skipClone"

  // SplittableDoFn
  val UseSplittableDoFn = "scio.parquet.read.useSplittableDoFn"
  private[scio] val UseSplittableDoFnDefault = true

  // Row Group API (note: this setting is only supported for SplittableDoFn-based reads)
  val FilterGranularity = "scio.parquet.read.filterGranularity"

  // Use Parquet's readNextFilteredRowGroup() API, which applies filters to entire pages within each row group
  val FilterGranularityPage = "page"

  // Use Parquet's readNextRowGroup() API, which applies filters per-record within each page
  val FilterGranularityRecord = "record"

  private[scio] def getUseSplittableDoFn(conf: Configuration, opts: PipelineOptions): Boolean = {
    Option(conf.get(UseSplittableDoFn)) match {
      case Some(v)                                => v.toBoolean
      case None if dataflowRunnerV2Disabled(opts) =>
        log.info(
          "Defaulting to HadoopFormatIO-based Parquet read as Dataflow Runner V2 is disabled. To opt in, " +
            "set `scio.parquet.read.useSplittableDoFn -> true` in your read Configuration."
        )
        false
      case None =>
        UseSplittableDoFnDefault
    }
  }

  private def dataflowRunnerV2Disabled(opts: PipelineOptions): Boolean =
    Option(opts.as(classOf[ExperimentalOptions]).getExperiments)
      .exists(_.contains("disable_runner_v2"))
}
