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

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.types._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.commons.io.FileUtils
import org.apache.parquet.filter2.predicate.FilterApi
import org.apache.parquet.io.api.Binary
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters._

case class Record(strField: String)

class ParquetReadFnTest extends PipelineSpec with BeforeAndAfterAll {
  private val testDir = Files.createTempDirectory("scio-test").toFile

  // Multiple row-groups
  private val multiRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 16)
  private val testMultiDir = new File(testDir, "multi")
  // Single row-group
  private val singleRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 1073741824)
  private val testSingleDir = new File(testDir, "single")

  private val typedRecords = (1 to 250).map(i => Record(i.toString)).toList

  override def beforeAll(): Unit = {
    val sc = ScioContext()
    val typedData = sc.parallelize(typedRecords)
    typedData.saveAsTypedParquetFile(testMultiDir.getAbsolutePath, conf = multiRowGroupConf)
    typedData.saveAsTypedParquetFile(testSingleDir.getAbsolutePath, conf = singleRowGroupConf)
    sc.run()
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  private def listFiles(dir: File): Seq[String] =
    Files
      .list(dir.toPath)
      .iterator()
      .asScala
      .map(_.toFile.toPath.toString)
      .toSeq

  "Parquet ReadFn" should "read at file-level granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val result = sc
      .parallelize(listFiles(testMultiDir))
      .readFiles(ParquetRead.readTypedFiles[Record](conf = granularityConf))

    result should containInAnyOrder(typedRecords)
    sc.run()
  }

  it should "read at file-level granularity for files with a single row group" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val result = sc
      .parallelize(listFiles(testMultiDir))
      .readFiles(ParquetRead.readTypedFiles[Record](conf = granularityConf))

    result should containInAnyOrder(typedRecords)
    sc.run()
  }

  it should "read at row-group granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val result = sc
      .typedParquetFile[Record](
        path = testMultiDir.getAbsolutePath,
        conf = granularityConf,
        suffix = ".parquet"
      )
    result should containInAnyOrder(typedRecords)
    sc.run()
  }

  it should "read at row-group granularity for files with a single row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val result = sc
      .typedParquetFile[Record](
        path = testSingleDir.getAbsolutePath,
        conf = granularityConf,
        suffix = ".parquet"
      )
    result should containInAnyOrder(typedRecords)
    sc.run()
  }

  "readTyped" should "work with a predicate" in {
    val sc = ScioContext()
    val result = sc
      .parallelize(listFiles(testSingleDir))
      .readFiles(
        ParquetRead.readTypedFiles[Record](
          FilterApi.eq(FilterApi.binaryColumn("strField"), Binary.fromString("1"))
        )
      )
    result should containSingleValue(Record("1"))
    sc.run()
  }

  it should "work with a predicate and projection" in {
    val sc = ScioContext()
    val result = sc
      .parallelize(listFiles(testSingleDir))
      .readFiles(
        ParquetRead.readTypedFiles[Record](
          FilterApi.eq(FilterApi.binaryColumn("strField"), Binary.fromString("1")),
          ParquetConfiguration.empty()
        )
      )
    result should containSingleValue(Record("1"))
    sc.run()
  }

  it should "be serializable" in {
    SerializableUtils.ensureSerializable(
      ParquetRead.readTypedFiles[Record](
        FilterApi.eq(FilterApi.binaryColumn("strField"), Binary.fromString("1")),
        ParquetConfiguration.empty()
      )
    )
  }

  "ParquetReadConfiguration" should "default to using splittableDoFn only if RunnerV2 experiment is enabled" in {
    // Default to true if RunnerV2 is set and user hasn't configured SDF explicitly
    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.empty(),
      PipelineOptionsFactory.fromArgs("--experiments=use_runner_v2,another_experiment").create()
    ) shouldBe true

    // Default to false if RunnerV2 is not set
    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.empty(),
      PipelineOptionsFactory.fromArgs("--experiments=another_experiment").create()
    ) shouldBe false

    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.empty(),
      PipelineOptionsFactory.fromArgs().create()
    ) shouldBe false

    // Respect user's configuration, if set
    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.of(ParquetReadConfiguration.UseSplittableDoFn -> false),
      PipelineOptionsFactory.fromArgs("--experiments=use_runner_v2").create()
    ) shouldBe false

    ParquetReadConfiguration.getUseSplittableDoFn(
      ParquetConfiguration.of(ParquetReadConfiguration.UseSplittableDoFn -> true),
      PipelineOptionsFactory.fromArgs().create()
    ) shouldBe true
  }
}
