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

import com.spotify.scio.ScioContext
import com.spotify.scio.avro.AvroUtils
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.transforms.ParDo
import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfterAll

import java.io.File
import java.nio.file.Files
import scala.jdk.CollectionConverters._

class ParquetMetadataDoFnTest extends PipelineSpec with BeforeAndAfterAll {
  private val tempDir = Files.createTempDirectory("parquet-metadata-test-").toFile
  private val testRecords = (1 to 100).map(AvroUtils.newGenericRecord)
  private val numRecords = testRecords.size

  // GCS bucket for integration test (set via environment variable or skip test)
  private val gcsTestBucket = sys.env.get("SCIO_TEST_GCS_BUCKET")

  override protected def beforeAll(): Unit = {
    // Write test data to local parquet file
    val sc = ScioContext()
    implicit val coder = com.spotify.scio.avro.avroGenericRecordCoder(AvroUtils.schema)
    sc.parallelize(testRecords)
      .saveAsParquetAvroFile(
        tempDir.getAbsolutePath,
        numShards = 1,
        schema = AvroUtils.schema
      )
    sc.run().waitUntilDone()
  }

  override protected def afterAll(): Unit = {
    FileUtils.deleteDirectory(tempDir)
  }

  private def getParquetFile: File = {
    val files = tempDir.listFiles()
      .filter(_.getName.endsWith(".parquet"))
    require(files.nonEmpty, "No parquet files found in test directory")
    files.head
  }

  // This test requires GCS credentials and a bucket to run
  // Set SCIO_TEST_GCS_BUCKET environment variable to enable
  "ParquetMetadataDoFn" should "read metadata from GCS parquet file" in {
    assume(gcsTestBucket.isDefined, "SCIO_TEST_GCS_BUCKET not set, skipping GCS integration test")

    val bucket = gcsTestBucket.get
    val gcsPath = s"gs://$bucket/parquet-metadata-test-${System.currentTimeMillis()}"

    try {
      // Write test data to GCS
      val writeContext = ScioContext()
      implicit val coder = com.spotify.scio.avro.avroGenericRecordCoder(AvroUtils.schema)
      writeContext
        .parallelize(testRecords)
        .saveAsParquetAvroFile(
          gcsPath,
          numShards = 1,
          schema = AvroUtils.schema
        )
      writeContext.run().waitUntilDone()

      // Find the written file
      val readContext = ScioContext()
      val gcsFilePattern = s"$gcsPath/*.parquet"
      val files = org.apache.beam.sdk.io.FileSystems
        .`match`(gcsFilePattern)
        .metadata()
        .asScala
        .map(_.resourceId().toString)
        .toSeq

      require(files.nonEmpty, "No parquet files found in GCS")

      // Read metadata using ParquetMetadataDoFn
      val metadataResults = readContext
        .parallelize(files)
        .applyTransform(ParDo.of(new ParquetMetadataDoFn))

      metadataResults should satisfySingleValue[(String, org.apache.parquet.hadoop.metadata.ParquetMetadata)] {
        case (filename, metadata) =>
          // Verify filename
          filename should startWith("gs://")
          filename should include(bucket)

          // Verify schema contains expected fields
          val schema = metadata.getFileMetaData.getSchema
          schema.getFieldCount should be > 0
          schema.containsField("int_field") shouldBe true
          schema.containsField("long_field") shouldBe true
          schema.containsField("string_field") shouldBe true

          // Verify row groups (blocks)
          val blocks = metadata.getBlocks
          blocks should not be empty

          // Verify total row count across all row groups
          val totalRows = blocks.asScala.map(_.getRowCount).sum
          totalRows shouldBe numRecords

          // Verify each block has reasonable size
          blocks.asScala.foreach { block =>
            block.getTotalByteSize should be > 0L
            block.getRowCount should be > 0L
          }

          true
      }

      readContext.run().waitUntilDone()
    } finally {
      // Clean up GCS files
      try {
        val cleanupContext = ScioContext()
        org.apache.beam.sdk.io.FileSystems
          .`match`(s"$gcsPath/*.parquet")
          .metadata()
          .asScala
          .foreach(m => org.apache.beam.sdk.io.FileSystems.delete(Seq(m.resourceId()).asJava))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  it should "verify schema fields in metadata" in {
    assume(gcsTestBucket.isDefined, "SCIO_TEST_GCS_BUCKET not set, skipping GCS integration test")

    val bucket = gcsTestBucket.get
    val gcsPath = s"gs://$bucket/parquet-schema-test-${System.currentTimeMillis()}"

    try {
      // Write test data
      val writeContext = ScioContext()
      implicit val coder = com.spotify.scio.avro.avroGenericRecordCoder(AvroUtils.schema)
      writeContext
        .parallelize(testRecords)
        .saveAsParquetAvroFile(gcsPath, numShards = 1, schema = AvroUtils.schema)
      writeContext.run().waitUntilDone()

      // Read and verify
      val readContext = ScioContext()
      val files = org.apache.beam.sdk.io.FileSystems
        .`match`(s"$gcsPath/*.parquet")
        .metadata()
        .asScala
        .map(_.resourceId().toString)
        .toSeq

      val metadataResults = readContext
        .parallelize(files)
        .applyTransform(ParDo.of(new ParquetMetadataDoFn))
        .map { case (_, metadata) =>
          val schema = metadata.getFileMetaData.getSchema
          (schema.getFieldCount, schema.toString)
        }

      metadataResults should satisfySingleValue[(Int, String)] { case (fieldCount, schemaStr) =>
        fieldCount shouldBe 7 // int, long, float, double, boolean, string, array
        schemaStr should include("int_field")
        schemaStr should include("long_field")
        schemaStr should include("float_field")
        schemaStr should include("double_field")
        schemaStr should include("boolean_field")
        schemaStr should include("string_field")
        schemaStr should include("array_field")
        true
      }

      readContext.run().waitUntilDone()
    } finally {
      // Cleanup
      try {
        org.apache.beam.sdk.io.FileSystems
          .`match`(s"$gcsPath/*.parquet")
          .metadata()
          .asScala
          .foreach(m => org.apache.beam.sdk.io.FileSystems.delete(Seq(m.resourceId()).asJava))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }

  it should "verify block metadata" in {
    assume(gcsTestBucket.isDefined, "SCIO_TEST_GCS_BUCKET not set, skipping GCS integration test")

    val bucket = gcsTestBucket.get
    val gcsPath = s"gs://$bucket/parquet-blocks-test-${System.currentTimeMillis()}"

    try {
      // Write test data
      val writeContext = ScioContext()
      implicit val coder = com.spotify.scio.avro.avroGenericRecordCoder(AvroUtils.schema)
      writeContext
        .parallelize(testRecords)
        .saveAsParquetAvroFile(gcsPath, numShards = 1, schema = AvroUtils.schema)
      writeContext.run().waitUntilDone()

      // Read and verify block metadata
      val readContext = ScioContext()
      val files = org.apache.beam.sdk.io.FileSystems
        .`match`(s"$gcsPath/*.parquet")
        .metadata()
        .asScala
        .map(_.resourceId().toString)
        .toSeq

      val blockStats = readContext
        .parallelize(files)
        .applyTransform(ParDo.of(new ParquetMetadataDoFn))
        .map { case (_, metadata) =>
          val blocks = metadata.getBlocks.asScala
          (
            blocks.size,
            blocks.map(_.getRowCount).sum,
            blocks.map(_.getTotalByteSize).sum,
            blocks.head.getColumns.size()
          )
        }

      blockStats should satisfySingleValue[(Int, Long, Long, Int)] {
        case (numBlocks, totalRows, totalBytes, numColumns) =>
          numBlocks should be > 0
          totalRows shouldBe numRecords
          totalBytes should be > 0L
          numColumns shouldBe 7 // 7 fields in schema
          true
      }

      readContext.run().waitUntilDone()
    } finally {
      // Cleanup
      try {
        org.apache.beam.sdk.io.FileSystems
          .`match`(s"$gcsPath/*.parquet")
          .metadata()
          .asScala
          .foreach(m => org.apache.beam.sdk.io.FileSystems.delete(Seq(m.resourceId()).asJava))
      } catch {
        case _: Exception => // Ignore cleanup errors
      }
    }
  }
}
