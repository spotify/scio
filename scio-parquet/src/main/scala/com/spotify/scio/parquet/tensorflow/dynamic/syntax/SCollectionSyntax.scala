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

package com.spotify.scio.parquet.tensorflow.dynamic.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO
import com.spotify.scio.parquet.tensorflow.dynamic.ParquetExampleSink
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.Schema
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.proto.example.Example

import scala.reflect.ClassTag

final class DynamicParquetExampleSCollectionOps(
  private val self: SCollection[Example]
) extends AnyVal {

  /**
   * Save this SCollection of [[Example]] records as a Parquet files written to dynamic
   * destinations.
   */
  def saveAsDynamicParquetExampleFile(
    path: String,
    schema: Schema,
    numShards: Int = ParquetExampleIO.WriteParam.DefaultNumShards,
    suffix: String = ParquetExampleIO.WriteParam.DefaultSuffix,
    compression: CompressionCodecName = ParquetExampleIO.WriteParam.DefaultCompression,
    conf: Configuration = ParquetExampleIO.WriteParam.DefaultConfiguration,
    tempDirectory: String = ParquetExampleIO.WriteParam.DefaultTempDirectory,
    prefix: String = ParquetExampleIO.WriteParam.DefaultPrefix
  )(
    destinationFn: Example => String
  )(implicit ct: ClassTag[Example], coder: Coder[Example]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Parquet example file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = new ParquetExampleSink(
        schema,
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
  implicit def dynamicParquetExampleSCollectionOps(
    sc: SCollection[Example]
  ): DynamicParquetExampleSCollectionOps =
    new DynamicParquetExampleSCollectionOps(sc)
}
