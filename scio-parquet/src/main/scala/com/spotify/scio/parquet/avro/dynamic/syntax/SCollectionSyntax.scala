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

package com.spotify.scio.parquet.avro.dynamic.syntax

import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro.{ParquetAvroIO, ParquetAvroSink}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

final class DynamicAvroGenericSCollectionOps(
  private val self: SCollection[GenericRecord]
) extends AnyVal {

  /** Save this SCollection of Avro records as a Parquet files written to dynamic destinations. */
  def saveAsDynamicParquetAvroFile(
    path: String,
    schema: Schema,
    numShards: Int = ParquetAvroIO.WriteParam.DefaultNumShards,
    suffix: String = ParquetAvroIO.WriteParam.DefaultSuffix,
    compression: CompressionCodecName = ParquetAvroIO.WriteParam.DefaultCompression,
    conf: Configuration = ParquetAvroIO.WriteParam.DefaultConfiguration,
    tempDirectory: String = ParquetAvroIO.WriteParam.DefaultTempDirectory,
    prefix: String = ParquetAvroIO.WriteParam.DefaultPrefix
  )(
    destinationFn: GenericRecord => String
  ): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Parquet avro file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = new ParquetAvroSink[GenericRecord](
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

final class DynamicParquetAvroSpecificSCollectionOps[T <: SpecificRecord](
  private val self: SCollection[T]
) extends AnyVal {

  /** Save this SCollection of Avro records as a Parquet files written to dynamic destinations. */
  def saveAsDynamicParquetAvroFile(
    path: String,
    numShards: Int = ParquetAvroIO.WriteParam.DefaultNumShards,
    suffix: String = ParquetAvroIO.WriteParam.DefaultSuffix,
    compression: CompressionCodecName = ParquetAvroIO.WriteParam.DefaultCompression,
    conf: Configuration = ParquetAvroIO.WriteParam.DefaultConfiguration,
    tempDirectory: String = ParquetAvroIO.WriteParam.DefaultTempDirectory,
    prefix: String = ParquetAvroIO.WriteParam.DefaultPrefix
  )(
    destinationFn: T => String
  )(implicit ct: ClassTag[T]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "Parquet avro file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val cls = ScioUtil.classOf[T]
      val schema = SpecificData.get().getSchema(cls)
      val sink =
        new ParquetAvroSink[T](
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
  implicit def dynamicParquetAvroGenericSCollectionOps(
    sc: SCollection[GenericRecord]
  ): DynamicAvroGenericSCollectionOps = new DynamicAvroGenericSCollectionOps(sc)

  implicit def dynamicParquetAvroSpecificSCollectionOps[T <: SpecificRecord](
    sc: SCollection[T]
  ): DynamicParquetAvroSpecificSCollectionOps[T] = new DynamicParquetAvroSpecificSCollectionOps(sc)
}
