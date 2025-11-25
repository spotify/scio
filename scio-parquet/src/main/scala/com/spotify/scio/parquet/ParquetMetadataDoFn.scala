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
import org.apache.parquet.hadoop.ParquetFileReader

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
    var reader: ParquetFileReader = null
    try {
      reader = ParquetFileReader.open(BeamInputFile.of(filePath))
      val md = reader.getFooter
      ParquetMetadata(
        schema = md.getFileMetaData.getSchema.toString,
        blocks = md.getBlocks.asScala.map { b =>
          BlockMetadata(
            rowCount = b.getRowCount,
            totalByteSize = b.getTotalByteSize,
            numColumns = b.getColumns.size()
          )
        }.toSeq,
        createdBy = md.getFileMetaData.getCreatedBy,
        numRows = md.getBlocks.asScala.map(_.getRowCount).sum,
        keyValueMetaData = Option(md.getFileMetaData.getKeyValueMetaData)
          .map(_.asScala.toMap)
          .getOrElse(Map.empty[String, String])
      )
    } finally {
      if (reader != null) reader.close()
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
