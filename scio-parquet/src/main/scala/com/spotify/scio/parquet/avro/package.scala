/*
 * Copyright 2019 Spotify AB.
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
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.avro.ParquetAvroIO.WriteParam
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/**
 * Main package for Parquet Avro APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.avro._
 * }}}
 */
package object avro {

  /** Alias for `me.lyh.parquet.avro.Projection`. */
  val Projection = me.lyh.parquet.avro.Projection

  /** Alias for `me.lyh.parquet.avro.Predicate`. */
  val Predicate = me.lyh.parquet.avro.Predicate

  /** Enhanced version of [[ScioContext]] with Parquet Avro methods. */
  implicit class ParquetAvroScioContext(@transient private val self: ScioContext) extends AnyVal {

    /**
     * Get an SCollection for a Parquet file as Avro records. Since Avro records produced by
     * Parquet column projection may be incomplete and may fail serialization, you must
     * [[ParquetAvroFile.map map]] the result to extract projected fields from the Avro records.
     *
     * Note that due to limitations of the underlying `HadoopInputFormatIO`,
     * Avro [[org.apache.avro.generic.GenericRecord GenericRecord]] and dynamic work rebalancing
     * are not supported. Without the latter, pipelines may not autoscale up or down during the
     * initial read and subsequent fused transforms.
     */
    def parquetAvroFile[T <: SpecificRecordBase: ClassTag](
      path: String,
      projection: Schema = null,
      predicate: FilterPredicate = null
    ): ParquetAvroFile[T] =
      self.requireNotClosed {
        new ParquetAvroFile[T](self, path, projection, predicate)
      }
  }

  class ParquetAvroFile[T: ClassTag] private[avro] (
    context: ScioContext,
    path: String,
    projection: Schema,
    predicate: FilterPredicate
  ) {
    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
     * Return a new SCollection by applying a function to all Parquet Avro records of this Parquet
     * file.
     */
    def map[U: ClassTag: Coder](f: T => U): SCollection[U] = {
      val param = ParquetAvroIO.ReadParam[T, U](projection, predicate, f)
      context.read(ParquetAvroIO[U](path))(param)
    }

    /**
     * Return a new SCollection by first applying a function to all Parquet Avro records of
     * this Parquet file, and then flattening the results.
     */
    def flatMap[U: ClassTag: Coder](f: T => TraversableOnce[U]): SCollection[U] =
      this
      // HadoopInputFormatIO does not support custom coder, force SerializableCoder
        .map(x => f(x).asInstanceOf[Serializable])
        .asInstanceOf[SCollection[TraversableOnce[U]]]
        .flatten

    private def toSCollection(implicit c: Coder[T]): SCollection[T] = {
      if (projection != null) {
        logger.warn(
          "Materializing Parquet Avro records with projection may cause " +
            "NullPointerException. Perform a `map` or `flatMap` immediately after " +
            "`parquetAvroFile` to map out projected fields."
        )
      }
      this.map(identity)
    }
  }

  object ParquetAvroFile {
    implicit def parquetAvroFileToSCollection[T: Coder](self: ParquetAvroFile[T]): SCollection[T] =
      self.toSCollection
    implicit def parquetAvroFileToParquetAvroSCollection[T: ClassTag: Coder](
      self: ParquetAvroFile[T]
    ): ParquetAvroSCollection[T] =
      new ParquetAvroSCollection(self.toSCollection)
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Parquet Avro
   * methods.
   */
  implicit class ParquetAvroSCollection[T](private val self: SCollection[T]) extends AnyVal {

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
      compression: CompressionCodecName = WriteParam.DefaultCompression
    )(implicit ct: ClassTag[T], coder: Coder[T]): ClosedTap[T] = {
      val param = WriteParam(schema, numShards, suffix, compression)
      self.write(ParquetAvroIO[T](path))(param)
    }
  }
}
