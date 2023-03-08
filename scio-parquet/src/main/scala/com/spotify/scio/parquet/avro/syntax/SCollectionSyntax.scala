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
import com.spotify.scio.parquet.avro.ParquetAvroIO.{ReadParam, WriteParam}
import com.spotify.scio.parquet.avro.ParquetAvroIO
import com.spotify.scio.parquet.read.{ParquetRead, ReadSupportFactory}
import com.spotify.scio.util.{FilenamePolicySupplier, Functions}
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.{Compression, FileIO}
import org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.transforms.{PTransform, ParDo}
import org.apache.beam.sdk.values.PCollection
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Parquet Avro
 * methods.
 */
class SCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  def readFilesWithFilename[A <: GenericRecord: Coder : ClassTag](
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    directoryTreatment: DirectoryTreatment = DirectoryTreatment.SKIP,
    compression: Compression = Compression.AUTO
  )(implicit ev: T <:< String): SCollection[(String, A)] = {
    val jobConf = Option(conf).getOrElse(new Configuration())
    // TODO ehhh crufty
    val param = ParquetAvroIO.ReadParam[A, A](identity, projection, predicate, conf)
    // TODO support legacy?
    ParquetAvroIO.splittableConfiguration(param, jobConf)

    val xxx: ParDo.SingleOutput[FileIO.ReadableFile, (String, A)] =
      ParquetRead
        .readFilesWithFilename[A, A](
          ReadSupportFactory.avro,
          new SerializableConfiguration(param.conf),
          Functions.serializableFn(identity)
        )

    self.covary_[String].readFiles[A](xxx, directoryTreatment, compression)
  }

  /**
   * Save this SCollection of Avro records as a Parquet file.
   * @param path
   *   output location of the write operation
   * @param schema
   *   must be not null if `T` is of type [[org.apache.avro.generic.GenericRecord GenericRecord]].
   * @param numShards
   *   number of shards per output directory
   * @param suffix
   *   defaults to .parquet
   * @param compression
   *   defaults to snappy
   */
  def saveAsParquetAvroFile(
    path: String,
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier
  )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
    val param = WriteParam(
      schema,
      numShards,
      suffix,
      compression,
      conf,
      shardNameTemplate,
      tempDirectory,
      filenamePolicySupplier
    )
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
