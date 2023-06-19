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

package com.spotify.scio.parquet.types.dynamic.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.types.{ParquetTypeIO, ParquetTypeSink}
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

final class DynamicParquetTypeSCollectionOps[T](
  private val self: SCollection[T]
) extends AnyVal {

  /** Save this SCollection of records as a Parquet files written to dynamic destinations. */
  def saveAsDynamicTypedParquetFile(
    path: String,
    numShards: Int = ParquetTypeIO.WriteParam.DefaultNumShards,
    suffix: String = ParquetTypeIO.WriteParam.DefaultSuffix,
    compression: CompressionCodecName = ParquetTypeIO.WriteParam.DefaultCompression,
    conf: Configuration = ParquetTypeIO.WriteParam.DefaultConfiguration,
    tempDirectory: String = ParquetTypeIO.WriteParam.DefaultTempDirectory,
    prefix: String = ParquetTypeIO.WriteParam.DefaultPrefix
  )(
    destinationFn: T => String
  )(implicit ct: ClassTag[T], coder: Coder[T], pt: ParquetType[T]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Typed parquet file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = new ParquetTypeSink[T](
        compression,
        new SerializableConfiguration(ParquetConfiguration.ofNullable(conf))
      )
      val write = writeDynamic(
        path = path,
        destinationFn = destinationFn,
        numShards = numShards,
        prefix = prefix,
        suffix = suffix,
        tempDirectory = tempDirectory
      ).via(sink)
      self.applyInternal(write)
    }
    ClosedTap[Nothing](EmptyTap)
  }
}

trait SCollectionSyntax {
  implicit def dynamicParquetTypeSCollectionOps[T](
    sc: SCollection[T]
  ): DynamicParquetTypeSCollectionOps[T] =
    new DynamicParquetTypeSCollectionOps(sc)
}
