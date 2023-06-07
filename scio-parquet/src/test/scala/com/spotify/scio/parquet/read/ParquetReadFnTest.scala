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
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.file.Files
import java.util.UUID

case class Record(strField: String)

class ParquetReadFnTest extends AnyFlatSpec with Matchers with BeforeAndAfterAll {
  private val testDir = Files.createTempDirectory("scio-test").toFile
  private val testMultiDir = new File(testDir, "multi")
  private val testSingleDir = new File(testDir, "single")
  private val records = (1 to 500).map(_ => Record(UUID.randomUUID().toString)).toList

  override def beforeAll(): Unit = {
    // Multiple row-groups
    val multiRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 16)

    // Single row-group
    val singleRowGroupConf = ParquetConfiguration.of("parquet.block.size" -> 1073741824)

    val sc = ScioContext()
    val data = sc.parallelize(records)
    data.saveAsTypedParquetFile(testMultiDir.getAbsolutePath, conf = multiRowGroupConf)
    data.saveAsTypedParquetFile(testSingleDir.getAbsolutePath, conf = singleRowGroupConf)

    sc.run()
  }

  override def afterAll(): Unit = FileUtils.deleteDirectory(testDir)

  "Parquet ReadFn" should "read at file-level granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](
        path = testMultiDir.getAbsolutePath,
        conf = granularityConf,
        suffix = ".parquet"
      )
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }

  it should "read at file-level granularity for files with a single row group" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityFile,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](
        path = testSingleDir.getAbsolutePath,
        conf = granularityConf,
        suffix = ".parquet"
      )
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }

  it should "read at row-group granularity for files with multiple row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](
        path = testMultiDir.getAbsolutePath,
        conf = granularityConf,
        suffix = ".parquet"
      )
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }

  it should "read at row-group granularity for files with a single row groups" in {
    val granularityConf = ParquetConfiguration.of(
      ParquetReadConfiguration.SplitGranularity -> ParquetReadConfiguration.SplitGranularityRowGroup,
      ParquetReadConfiguration.UseSplittableDoFn -> true
    )

    val sc = ScioContext()
    val tap = sc
      .typedParquetFile[Record](
        path = testSingleDir.getAbsolutePath,
        conf = granularityConf,
        suffix = ".parquet"
      )
      .materialize

    val readElements = sc.run().waitUntilDone().tap(tap).value.toList
    readElements.size should equal(500)
    readElements should contain theSameElementsAs records
  }
}
