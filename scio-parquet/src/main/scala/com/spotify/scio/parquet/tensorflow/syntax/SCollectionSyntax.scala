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

package com.spotify.scio.parquet.tensorflow.syntax

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO.WriteParam
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.Schema
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.proto.example.Example

/** Enhanced version of [[SCollection]] with Parquet [[Example]] methods. */
final class SCollectionOps(private val self: SCollection[Example]) extends AnyVal {

  /** Save this SCollection of [[Example]] records as a Parquet file. */
  def saveAsParquetExampleFile(
    path: String,
    schema: Schema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix
  ): ClosedTap[Example] =
    self.write(ParquetExampleIO(path))(
      WriteParam(
        schema,
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
  implicit def parquetExampleSCollection(c: SCollection[Example]): SCollectionOps =
    new SCollectionOps(c)
}
