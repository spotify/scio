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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.avro.ParquetAvroIO
import com.spotify.scio.parquet.avro.ParquetAvroIO.ReadParam
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag

/** Enhanced version of [[ScioContext]] with Parquet Avro methods. */
final class ScioContextOps(@transient private val self: ScioContext) extends AnyVal {

  /**
   * Get an SCollection for a Parquet file as Avro records. Since Avro records produced by Parquet
   * column projection may be incomplete and may fail serialization, you must
   * [[ParquetAvroFile.map map]] the result to extract projected fields from the Avro records.
   *
   * Note that due to limitations of the underlying `HadoopInputFormatIO`, dynamic work rebalancing
   * is not supported. Pipelines may not autoscale up or down during the initial read and subsequent
   * fused transforms.
   */
  def parquetAvroFile[T <: GenericRecord: ClassTag](
    path: String,
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = ReadParam.DefaultSuffix
  ): ParquetAvroFile[T] =
    self.requireNotClosed {
      new ParquetAvroFile[T](self, path, projection, predicate, conf, suffix)
    }
}

class ParquetAvroFile[T: ClassTag] private[avro] (
  context: ScioContext,
  path: String,
  projection: Schema,
  predicate: FilterPredicate,
  conf: Configuration,
  suffix: String
) {
  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Return a new SCollection by applying a function to all Parquet Avro records of this Parquet
   * file.
   */
  def map[U: ClassTag: Coder](f: T => U): SCollection[U] = {
    val param = ParquetAvroIO.ReadParam[T, U](f, projection, predicate, conf, suffix)
    context.read(ParquetAvroIO[U](path))(param)
  }

  /**
   * Return a new SCollection by first applying a function to all Parquet Avro records of this
   * Parquet file, and then flattening the results.
   */
  def flatMap[U: Coder](f: T => TraversableOnce[U]): SCollection[U] =
    this
      // HadoopInputFormatIO does not support custom coder, force SerializableCoder
      .map(x => f(x).asInstanceOf[Serializable])
      .asInstanceOf[SCollection[TraversableOnce[U]]]
      .flatten

  private[avro] def toSCollection(implicit c: Coder[T]): SCollection[T] = {
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

trait ScioContextSyntax {
  implicit def parquetAvroScioContextOps(c: ScioContext): ScioContextOps = new ScioContextOps(c)
  implicit def parquetAvroFileToSCollection[T: Coder](self: ParquetAvroFile[T]): SCollection[T] =
    self.toSCollection
}
