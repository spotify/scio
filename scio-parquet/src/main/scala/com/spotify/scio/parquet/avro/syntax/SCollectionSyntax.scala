/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.parquet.avro.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.avro.ParquetAvroIO.WriteParam
import com.spotify.scio.parquet.avro.ParquetAvroIO
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.values.WindowingStrategy
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Parquet Avro
 * methods.
 */
class SCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection of Avro records as a Parquet file.
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   */
  def saveAsParquetAvroFile(
    path: String,
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration
  )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = WriteParam(schema, numShards, suffix, compression, None, conf)
    self.write(ParquetAvroIO[T](path))(param)
  }

  /**
   * Save this SCollection of Avro records as a Parquet files written to dynamic destinations.
   * @param path output location of the write operation
   * @param filenameFunction an Either representing one of two functions which generates a filename. The Either must
   *                         be a Left when writing dynamic files from windowed SCollections, or a Right when writing
   *                         dynamic files from un-windowed SCollections. When the Either is a Left, the function's
   *                         arguments represent
   *                            (the shard number,
   *                            the total number of shards,
   *                            the bounded window,
   *                            the pane info for the window)
   *                         When the Either is a Right, the function's arguments represent
   *                            (the shard number,
   *                            the total number of shards)
   * @param schema must be not null if `T` is of type
   *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @param numShards number of shards per output directory
   * @param suffix defaults to .parquet
   * @param compression defaults to snappy
   */
  def saveAsDynamicParquetAvroFile(
    path: String,
    filenameFunction: Either[(Int, Int, BoundedWindow, PaneInfo) => String, (Int, Int) => String],
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration
  )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    if (
      (self.internal.getWindowingStrategy != WindowingStrategy
        .globalDefault() && filenameFunction.isRight) ||
      (self.internal.getWindowingStrategy == WindowingStrategy
        .globalDefault() && filenameFunction.isLeft)
    ) {
      throw new NotImplementedError(
        "The filenameFunction value passed to saveAsDynamicParquetAvroFile does not" +
          " support the window strategy applied to the SCollection."
      )
    }

    val param =
      WriteParam(schema, numShards, suffix, compression, Some(filenameFunction), conf)
    self.write(ParquetAvroIO[T](path))(param)
  }
}

trait SCollectionSyntax {
  implicit def parquetAvroSCollectionOps[T](c: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps[T](c)
  implicit def parquetAvroSCollection[T: ClassTag: Coder](
    self: ParquetAvroFile[T]
  ): SCollectionOps[T] =
    new SCollectionOps[T](self.toSCollection)
}
