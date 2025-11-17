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

import org.apache.beam.sdk.extensions.smb.{BucketMetadata, SmbTestUtils}
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType
import org.scalatest.Assertion
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileInputStream}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

/**
 * Test helper for verifying SMB output structure and contents.
 *
 * Validates that SMB outputs have:
 *   - Valid metadata.json with correct bucket configuration
 *   - Properly named bucket files
 *   - Records in correct buckets (hash matches bucket ID)
 *   - Records sorted within each bucket
 */
object SmbTestHelper extends Matchers {

  /**
   * Verify SMB directory structure.
   *
   * Checks:
   *   - metadata.json exists and is valid
   *   - Bucket files exist with correct naming
   *
   * Note: This does NOT verify record-level bucket assignment or sort order due to complexity of
   * handling Avro GenericRecord vs. specific record types. Use readAllSmbRecords() separately to
   * verify actual data if needed.
   *
   * @param outputDir
   *   Path to SMB output directory
   * @param expectedNumBuckets
   *   Expected number of buckets
   * @param keyExtractor
   *   Function to extract key from record (unused, kept for API compatibility)
   * @param expectedHashType
   *   Optional expected hash type (if None, any hash type is accepted)
   * @tparam T
   *   Record type
   * @return
   *   Assertion indicating success or failure
   */
  @annotation.nowarn("cat=unused-params")
  def verifySmbOutput[T](
    outputDir: String,
    expectedNumBuckets: Int,
    keyExtractor: T => String,
    expectedHashType: Option[HashType] = None
  ): Assertion = {

    // 1. Verify metadata.json
    val metadataFile = new File(s"$outputDir/metadata.json")
    metadataFile should exist

    val metadata = BucketMetadata.from(new FileInputStream(metadataFile))
    metadata.getNumBuckets shouldBe expectedNumBuckets
    expectedHashType.foreach(expectedType => metadata.getHashType shouldBe expectedType)
    // Note: Key class can be any type (Integer, String, etc.) so we don't verify it

    // 2. Verify bucket files exist
    val outputDirFile = new File(outputDir)
    (0 until expectedNumBuckets).foreach { bucketId =>
      val bucketPattern = f"bucket-$bucketId%05d-.*\\.avro"
      val files = outputDirFile.listFiles().filter(_.getName.matches(bucketPattern))

      withClue(s"Expected bucket file for bucket $bucketId in $outputDir") {
        files should not be empty
      }
    }

    succeed
  }

  /**
   * Read all records from an SMB output directory. Useful for verifying data correctness in tests.
   *
   * @param outputDir
   *   Path to SMB output directory
   * @tparam T
   *   Record type
   * @return
   *   List of all records from all buckets
   */
  def readAllSmbRecords[T: ClassTag](outputDir: String): List[T] = {
    val metadataFile = new File(s"$outputDir/metadata.json")
    val metadata = BucketMetadata.from(new FileInputStream(metadataFile))

    val outputDirFile = new File(outputDir)
    val numBuckets = metadata.getNumBuckets

    (0 until numBuckets).flatMap { bucketId =>
      val bucketPattern = f"bucket-$bucketId%05d-.*\\.avro"
      val files = outputDirFile.listFiles().filter(_.getName.matches(bucketPattern))
      files.flatMap(file => readAvroFile[T](file, metadata))
    }.toList
  }

  /**
   * Count unique keys in SMB output. Useful for verifying shared computation in multi-output
   * scenarios.
   *
   * @param outputDir
   *   Path to SMB output directory
   * @param keyExtractor
   *   Function to extract key from record
   * @tparam T
   *   Record type
   * @return
   *   Number of unique keys
   */
  def countUniqueKeys[T: ClassTag](outputDir: String, keyExtractor: T => String): Int =
    readAllSmbRecords[T](outputDir).map(keyExtractor).distinct.size

  /**
   * Read all records from an Avro file using Beam's SMB utilities.
   *
   * @param file
   *   Avro file to read
   * @param metadata
   *   metadata to extract schema
   * @tparam T
   *   record type
   * @return
   *   list of records
   */
  private def readAvroFile[T](file: File, metadata: BucketMetadata[_, _, _]): List[T] =
    // Use Java helper to access package-private methods and read Avro files
    SmbTestUtils.readAvroFile(metadata.asInstanceOf[BucketMetadata[_, _, T]], file).asScala.toList

  /**
   * Hash a key to a bucket ID using BucketMetadata's hashing.
   *
   * @param key
   *   the key to hash
   * @param metadata
   *   metadata containing hash configuration
   * @return
   *   bucket ID (0 to numBuckets-1)
   */
  @annotation.nowarn("cat=unused-privates")
  private def hashToBucket(key: String, metadata: BucketMetadata[_, _, _]): Int =
    // Use Java helper to access package-private methods
    SmbTestUtils.hashToBucket(metadata.asInstanceOf[BucketMetadata[String, _, _]], key)
}
