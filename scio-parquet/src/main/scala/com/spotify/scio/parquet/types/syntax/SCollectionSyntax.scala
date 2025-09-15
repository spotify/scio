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

package com.spotify.scio.parquet.types.syntax

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.types.ParquetTypeIO
import com.spotify.scio.parquet.types.ParquetTypeIO.WriteParam
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.reflect.ClassTag

/** Enhanced version of [[SCollection]] with Parquet type-safe methods. */
final class SCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /** Save this SCollection of case classes `T` as a Parquet file. */
  def saveAsTypedParquetFile(
    path: String,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix
  )(implicit ct: ClassTag[T], coder: Coder[T], pt: ParquetType[T]): ClosedTap[T] =
    self.write(ParquetTypeIO[T](path))(
      WriteParam(
        numShards,
        suffix,
        compression,
        conf,
        filenamePolicySupplier,
        prefix,
        shardNameTemplate,
        tempDirectory
      )
    )
}

trait SCollectionSyntax {
  implicit def parquetTypeSCollection[T](c: SCollection[T]): SCollectionOps[T] =
    new SCollectionOps[T](c)
}
