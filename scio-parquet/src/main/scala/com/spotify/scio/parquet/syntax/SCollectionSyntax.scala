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

package com.spotify.scio.parquet.syntax

import com.spotify.scio.parquet.{BeamInputFile, ParquetBlockMetadata, ParquetMetadata}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.{io => beam}
import org.apache.parquet.hadoop.ParquetFileReader

import scala.jdk.CollectionConverters._
import scala.util.Try

trait SCollectionSyntax {
  implicit def parquetStringSCollectionOps(
    scoll: SCollection[String]
  ): ParquetStringSCollectionSyntax =
    new ParquetStringSCollectionSyntax(scoll)
  implicit def parquetReadableFileSCollectionOps(
    scoll: SCollection[ReadableFile]
  ): ParquetReadableFileSCollectionSyntax =
    new ParquetReadableFileSCollectionSyntax(scoll)
}

class ParquetReadableFileSCollectionSyntax(self: SCollection[ReadableFile]) {
  def parquetMetadata: SCollection[(String, Try[ParquetMetadata])] = {
    self.transform { s =>
      s.map { rf =>
        rf.getMetadata.resourceId().toString -> Try {
          val seekable = rf.openSeekable()
          var reader: ParquetFileReader = null
          try {
            val bif = BeamInputFile.of(seekable)
            reader = ParquetFileReader.open(bif)
            val md = reader.getFooter
            ParquetMetadata(
              md.getFileMetaData.getSchema.toString,
              md.getBlocks.asScala.map { b =>
                ParquetBlockMetadata(
                  b.getRowCount,
                  b.getTotalByteSize,
                  b.getColumns.size()
                )
              }.toSeq,
              md.getFileMetaData.getCreatedBy,
              md.getBlocks.asScala.map(_.getRowCount).sum,
              Option(md.getFileMetaData.getKeyValueMetaData)
                .map(_.asScala.toMap)
                .getOrElse(Map.empty[String, String])
            )
          } finally {
            seekable.close()
            if (reader != null) reader.close()
          }
        }
      }
    }
  }
}

class ParquetStringSCollectionSyntax(self: SCollection[String]) {
  def parquetMetadata(
    directoryTreatment: DirectoryTreatment = DirectoryTreatment.SKIP,
    compression: Compression = Compression.AUTO
  ): SCollection[(String, Try[ParquetMetadata])] = {
    self.transform { s =>
      new ParquetReadableFileSCollectionSyntax(
        s.applyTransform(beam.FileIO.matchAll())
          .applyTransform(
            beam.FileIO
              .readMatches()
              .withCompression(compression)
              .withDirectoryTreatment(directoryTreatment)
          )
      ).parquetMetadata
    }
  }
}
