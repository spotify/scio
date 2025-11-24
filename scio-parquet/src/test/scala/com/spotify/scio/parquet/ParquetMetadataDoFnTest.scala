/*
 * Copyright 2025 Spotify AB.
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

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.transforms.ParDo
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files
import scala.util.{Failure, Success, Try}

class ParquetMetadataDoFnTest extends PipelineSpec with BeforeAndAfterAll {
  import ParquetMetadataDoFn._

  private val tempDir = Files.createTempDirectory("parquet-metadata-test-").toFile
  private val testRecords = (1 to 100).map(AvroUtils.newGenericRecord)
  private val numRecords = testRecords.size

  override protected def beforeAll(): Unit = {
    // Write test data to local parquet file
    val sc = ScioContext()
    implicit val coder: com.spotify.scio.coders.Coder[org.apache.avro.generic.GenericRecord] =
      com.spotify.scio.avro.avroGenericRecordCoder(AvroUtils.schema)
    sc.parallelize(testRecords)
      .saveAsParquetAvroFile(
        tempDir.getAbsolutePath,
        numShards = 1,
        schema = AvroUtils.schema
      )
    sc.run().waitUntilDone()
  }

  override protected def afterAll(): Unit =
    FileUtils.deleteDirectory(tempDir)

  private def getParquetFile: File = {
    val files = tempDir
      .listFiles()
      .filter(_.getName.endsWith(".parquet"))
    require(files.nonEmpty, "No parquet files found in test directory")
    files.head
  }

  "ParquetMetadataDoFn" should "read and parse parquet file metadata" in {
    val parquetFile = getParquetFile
    val filePath = parquetFile.getAbsolutePath

    val sc = ScioContext()
    val metadataResults = sc
      .parallelize(Seq(filePath))
      .applyTransform(ParDo.of(new ParquetMetadataDoFn))

    metadataResults should satisfySingleValue[(String, Try[ParquetMetadata])] {
      case (filename, Success(metadata)) =>
        filename shouldBe filePath

        // Verify schema contains all expected fields
        List(
          "int_field",
          "long_field",
          "float_field",
          "double_field",
          "boolean_field",
          "string_field",
          "array_field"
        ).foreach { f =>
          assert(metadata.schema.contains(f), s"schema should contain $f")
        }

        // Verify row count
        metadata.numRows shouldBe numRecords

        // Verify block metadata
        val numBlocks = metadata.blocks.size
        val totalBytes = metadata.blocks.map(_.totalByteSize).sum
        assert(numBlocks > 0, s"numBlocks should be > 0 but was $numBlocks")
        assert(totalBytes > 0L, s"totalBytes should be > 0 but was $totalBytes")

        metadata.blocks.foreach { block =>
          assert(
            block.totalByteSize > 0L,
            s"totalByteSize should be > 0 but was ${block.totalByteSize}"
          )
          assert(block.rowCount > 0L, s"rowCount should be > 0 but was ${block.rowCount}")
          assert(block.numColumns == 7, s"numColumns should be 7 but was ${block.numColumns}")
        }

        // Verify key-value metadata exists
        assert(metadata.keyValueMetaData != null, "keyValueMetaData should not be null")

        true
      case (filename, Failure(error)) =>
        fail(s"Expected Success but got Failure for $filename: ${error.getMessage}")
    }

    sc.run().waitUntilDone()
  }

  it should "handle errors gracefully for invalid files" in {
    val sc = ScioContext()
    val invalidPath = "/path/to/nonexistent/file.parquet"

    val results = sc
      .parallelize(Seq(invalidPath))
      .applyTransform(ParDo.of(new ParquetMetadataDoFn))

    results should satisfySingleValue[(String, Try[ParquetMetadata])] {
      case (filename, Failure(error)) =>
        filename shouldBe invalidPath
        error shouldBe a[Throwable]
        true
      case (filename, Success(_)) =>
        fail(s"Expected Failure but got Success for invalid path $filename")
    }

    sc.run().waitUntilDone()
  }
}
