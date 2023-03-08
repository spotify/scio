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

import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.transforms.{PTransform, ParDo, SerializableFunction}
import org.apache.beam.sdk.values.{PBegin, PCollection}

object ParquetRead {

  def read[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    filePattern: String,
    projectionFn: SerializableFunction[T, R]
  ): PTransform[PBegin, PCollection[R]] =
    new PTransform[PBegin, PCollection[R]] {
      override def expand(input: PBegin): PCollection[R] = {
        input
          .apply(FileIO.`match`().filepattern(filePattern))
          .apply(FileIO.readMatches)
          .apply(readFiles(readSupportFactory, conf, projectionFn))
      }
    }

  def readFiles[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    projectionFn: SerializableFunction[T, R]
  ): ParDo.SingleOutput[ReadableFile, R] = {
    val sdf = new ParquetReadFn[T, R](readSupportFactory, conf, projectionFn)
    ParDo.of(sdf)
  }

  def readFilesWithFilename[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    projectionFn: SerializableFunction[T, R]
  ): ParDo.SingleOutput[ReadableFile, (String, R)] = {
    val sdf = new FilenameRetainingParquetReadFn[T, R](readSupportFactory, conf, projectionFn)
    ParDo.of(sdf)
  }

}
