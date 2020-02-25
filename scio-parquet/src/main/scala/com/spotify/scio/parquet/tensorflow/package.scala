/*
 * Copyright 2020 Spotify AB.
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

import com.spotify.scio.ScioContext
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.parquet.tensorflow.ParquetExampleIO.WriteParam
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.Schema
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.example.Example

/**
 * Main package for Parquet TensorFlow APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.tensorflow._
 * }}}
 */
package object tensorflow {

  /** Enhanced version of [[ScioContext]] with Parquet [[Example]] methods. */
  implicit class ParquetExampleScioContext(private val self: ScioContext) extends AnyVal {

    /**
     * Get an SCollection for a Parquet file as [[Example]] records.
     *
     * Note that due to limitations of the underlying `HadoopInputFormatIO`,
     * Avro [[org.apache.avro.generic.GenericRecord GenericRecord]] and dynamic work rebalancing
     * are not supported. Without the latter, pipelines may not autoscale up or down during the
     * initial read and subsequent fused transforms.
     */
    def parquetExampleFile(
      path: String,
      projection: Seq[String] = null,
      predicate: FilterPredicate = null
    ): SCollection[Example] =
      self.read(ParquetExampleIO(path))(ParquetExampleIO.ReadParam(projection, predicate))
  }

  /** Enhanced version of [[SCollection]] with Parquet [[Example]] methods. */
  implicit class ParquetExampleSCollection(private val self: SCollection[Example]) extends AnyVal {

    /** Save this SCollection of [[Example]] records as a Parquet file. */
    def saveAsParquetExampleFile(
      path: String,
      schema: Schema,
      numShards: Int = WriteParam.DefaultNumShards,
      suffix: String = WriteParam.DefaultSuffix,
      compression: CompressionCodecName = WriteParam.DefaultCompression
    ): ClosedTap[Example] =
      self.write(ParquetExampleIO(path))(WriteParam(schema, numShards, suffix, compression))
  }
}
