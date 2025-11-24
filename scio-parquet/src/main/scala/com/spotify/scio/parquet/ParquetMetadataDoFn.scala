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

import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.parquet.format.converter.ParquetMetadataConverter

import java.io.ByteArrayInputStream
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.channels.{ReadableByteChannel, SeekableByteChannel}
import scala.jdk.CollectionConverters._
import scala.util.Try

object ParquetMetadataDoFn {
  case class BlockMetadata(
    rowCount: Long,
    totalByteSize: Long,
    numColumns: Int
  )

  case class ParquetMetadata(
    schema: String,
    blocks: Seq[BlockMetadata],
    createdBy: String,
    numRows: Long,
    keyValueMetaData: Map[String, String]
  )
}

/** Reads a parquet file and emits its metadata */
class ParquetMetadataDoFn extends DoFn[String, (String, Try[ParquetMetadataDoFn.ParquetMetadata])] {
  import ParquetMetadataDoFn._
  private val MagicBytes = "PAR1".getBytes

  @ProcessElement
  def processElement(
    @Element filePath: String,
    out: OutputReceiver[(String, Try[ParquetMetadata])]
  ): Unit = out.output((filePath, Try(readMetadata(filePath))))

  private def readMetadata(filePath: String): ParquetMetadata = {
    val resourceId = FileSystems.matchSingleFileSpec(filePath).resourceId()
    val fileSize = FileSystems.matchSingleFileSpec(filePath).sizeBytes()

    val rbc: ReadableByteChannel = FileSystems.open(resourceId)
    try {
      val channel = rbc match {
        case seekable: SeekableByteChannel => seekable
        case _                             =>
          throw new IllegalArgumentException(s"Filesystem does not support seek. Path: $filePath")
      }

      // parquet files suffixed with "PAR1" (4 bytes) and a 4-byte footer length
      val footerBytes = readBytesAt(channel, fileSize - 8, 8)
      if (!java.util.Arrays.equals(footerBytes.slice(4, 8), MagicBytes)) {
        throw new IllegalArgumentException(s"Invalid parquet file. Path: $filePath")
      }
      // extract the little-endian footer length
      val buffer = ByteBuffer.wrap(footerBytes, 0, 4)
      buffer.order(ByteOrder.LITTLE_ENDIAN)
      val footerLength = buffer.getInt()
      val bytes = readBytesAt(channel, fileSize - 8 - footerLength, footerLength)
      val metadata = new ParquetMetadataConverter()
        .readParquetMetadata(new ByteArrayInputStream(bytes), ParquetMetadataConverter.NO_FILTER)

      ParquetMetadata(
        schema = metadata.getFileMetaData.getSchema.toString,
        blocks = metadata.getBlocks.asScala.map { b =>
          BlockMetadata(
            rowCount = b.getRowCount,
            totalByteSize = b.getTotalByteSize,
            numColumns = b.getColumns.size()
          )
        }.toSeq,
        createdBy = metadata.getFileMetaData.getCreatedBy,
        numRows = metadata.getBlocks.asScala.map(_.getRowCount).sum,
        keyValueMetaData = Option(metadata.getFileMetaData.getKeyValueMetaData)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty[String, String])
      )
    } finally {
      if (rbc != null) rbc.close()
    }
  }

  private def readBytesAt(
    channel: SeekableByteChannel,
    position: Long,
    length: Int
  ): Array[Byte] = {
    channel.position(position)
    val buffer = ByteBuffer.allocate(length)
    var bytesRead = 0
    while (bytesRead < length) {
      val read = channel.read(buffer)
      if (read == -1) {
        throw new IllegalStateException(
          s"Unexpected EOF at position $position, expected $length bytes but got $bytesRead"
        )
      }
      bytesRead += read
    }
    buffer.array()
  }
}
