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

import com.google.cloud.storage.{Blob, BlobId, Storage, StorageOptions}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.parquet.format.converter.ParquetMetadataConverter
import org.apache.parquet.hadoop.metadata.ParquetMetadata
import org.slf4j.LoggerFactory

import java.io.ByteArrayInputStream
import java.nio.{ByteBuffer, ByteOrder}

/**
 * A [[DoFn]] that reads only the metadata section of a Parquet file from GCS using efficient range
 * reads to avoid downloading the entire file.
 *
 * The DoFn takes GCS URIs (gs://bucket/path) as input and outputs a tuple of (filename, metadata)
 * for each file.
 */
class ParquetMetadataDoFn extends DoFn[String, (String, ParquetMetadata)] {
  @transient
  private lazy val logger = LoggerFactory.getLogger(classOf[ParquetMetadataDoFn])

  @transient
  private lazy val storage: Storage = StorageOptions.getDefaultInstance.getService

  private val ParquetMagic = "PAR1".getBytes
  private val FooterLengthSize = 4
  private val MagicLength = 4

  @ProcessElement
  def processElement(
    @Element gcsUri: String,
    out: OutputReceiver[(String, ParquetMetadata)]
  ): Unit = {
    logger.debug(s"Reading Parquet metadata from: $gcsUri")

    val (bucketName, blobName) = parseGcsUri(gcsUri)
    val metadata = readMetadata(bucketName, blobName)

    out.output((gcsUri, metadata))
  }

  private def parseGcsUri(uri: String): (String, String) = {
    val cleaned = uri.stripPrefix("gs://")
    val parts = cleaned.split("/", 2)
    require(parts.length == 2, s"Invalid GCS URI: $uri")
    (parts(0), parts(1))
  }

  private def readMetadata(bucketName: String, blobName: String): ParquetMetadata = {
    val blobId = BlobId.of(bucketName, blobName)
    val blob = storage.get(blobId)

    require(blob != null && blob.exists(), s"Blob not found: gs://$bucketName/$blobName")

    val fileSize = blob.getSize

    // Step 1: Read the last 8 bytes to get footer length
    // Format: 4 bytes footer length + 4 bytes magic "PAR1"
    val footerReadStart = fileSize - MagicLength - FooterLengthSize
    val footerLengthBytes = blob.getContent(
      Storage.BlobSourceOption.offset(footerReadStart),
      Storage.BlobSourceOption.limit(FooterLengthSize + MagicLength)
    )

    // Verify magic bytes
    val magicBytes = new Array[Byte](MagicLength)
    System.arraycopy(footerLengthBytes, FooterLengthSize, magicBytes, 0, MagicLength)
    require(
      java.util.Arrays.equals(magicBytes, ParquetMagic),
      "Not a valid Parquet file - magic bytes mismatch"
    )

    // Extract footer length (little-endian)
    val buffer = ByteBuffer.wrap(footerLengthBytes, 0, FooterLengthSize)
    buffer.order(ByteOrder.LITTLE_ENDIAN)
    val footerLength = buffer.getInt()

    logger.debug(s"Parquet footer length: $footerLength bytes")

    // Step 2: Read the actual footer metadata
    val metadataStart = fileSize - MagicLength - FooterLengthSize - footerLength
    val metadataBytes = blob.getContent(
      Storage.BlobSourceOption.offset(metadataStart),
      Storage.BlobSourceOption.limit(footerLength)
    )

    // Step 3: Parse the metadata using Parquet libraries
    val converter = new ParquetMetadataConverter()
    val fileMetaData = ParquetMetadataConverter.readFileMetaData(
      new ByteArrayInputStream(metadataBytes)
    )

    converter.fromParquetMetadata(fileMetaData)
  }
}
