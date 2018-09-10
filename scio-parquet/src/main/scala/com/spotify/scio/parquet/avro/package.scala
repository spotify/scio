/*
 * Copyright 2017 Spotify AB.
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
import com.spotify.scio.io.Tap
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.language.implicitConversions
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
  implicit class ParquetAvroScioContext(val self: ScioContext) extends AnyVal {

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
    def parquetAvroFile[T <: SpecificRecordBase : ClassTag](path: String,
                                                            projection: Schema = null,
                                                            predicate: FilterPredicate = null)
    : ParquetAvroFile[T] = self.requireNotClosed {
      new ParquetAvroFile[T](self, path, projection, predicate)
    }

  }

  class ParquetAvroFile[T: ClassTag] private[avro](context: ScioContext,
                                                   path: String,
                                                   projection: Schema,
                                                   predicate: FilterPredicate) {

    private val logger = LoggerFactory.getLogger(this.getClass)

    /**
     * Return a new SCollection by applying a function to all Parquet Avro records of this Parquet
     * file.
     */
    def map[U: ClassTag](f: T => U): SCollection[U] = {
      val param = ParquetAvroIO.ReadParam[T, U](projection, predicate, f)
      context.read(ParquetAvroIO[U](path))(param)
    }

    /**
     * Return a new SCollection by first applying a function to all Parquet Avro records of
     * this Parquet file, and then flattening the results.
     */
    def flatMap[U: ClassTag](f: T => TraversableOnce[U]): SCollection[U] =
      this
        // HadoopInputFormatIO does not support custom coder, force SerializableCoder
        .map(x => f(x).asInstanceOf[Serializable])
        .asInstanceOf[SCollection[TraversableOnce[U]]]
        .flatten

    private def toSCollection: SCollection[T] = {
      if (projection != null) {
        logger.warn("Materializing Parquet Avro records with projection may cause " +
          "NullPointerException. Perform a `map` or `flatMap` immediately after " +
          "`parquetAvroFile` to map out projected fields.")
      }
      this.map(identity)
    }

    private def setInputPaths(job: Job, path: String): Unit = {
      // This is needed since `FileInputFormat.setInputPaths` validates paths locally and requires
      // the user's GCP credentials.
      GcsConnectorUtil.setCredentials(job)

      FileInputFormat.setInputPaths(job, path)

      // It will interfere with credentials in Dataflow workers
      if (!ScioUtil.isLocalRunner(context.options.getRunner)) {
        GcsConnectorUtil.unsetCredentials(job)
      }
    }

  }

  object ParquetAvroFile {
    implicit def parquetAvroFileToSCollection[T: ClassTag](self: ParquetAvroFile[T])
    : SCollection[T] = self.toSCollection
    implicit def parquetAvroFileToParquetAvroSCollection[T: ClassTag](self: ParquetAvroFile[T])
    : ParquetAvroSCollection[T] = new ParquetAvroSCollection(self.toSCollection)
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Parquet Avro
   * methods.
   */
  implicit class ParquetAvroSCollection[T](val self: SCollection[T]) extends AnyVal {
    /**
     * Save this SCollection of Avro records as a Parquet file.
     * @param schema must be not null if `T` is of type
     *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
     */
    def saveAsParquetAvroFile(path: String,
                              schema: Schema = null,
                              numShards: Int = 0,
                              suffix: String = "",
                              compression: CompressionCodecName = CompressionCodecName.SNAPPY)
    : Future[Tap[T]] = {
      import self.ct
      val param = ParquetAvroIO.WriteParam(schema, numShards, suffix, compression)
      self.write(ParquetAvroIO[T](path))(param)
    }
  }

  private[avro] object GcsConnectorUtil {
    def setCredentials(job: Job): Unit = {
      // These are needed since `FileInputFormat.setInputPaths` validates paths locally and
      // requires the user's GCP credentials.
      sys.env.get("GOOGLE_APPLICATION_CREDENTIALS") match {
        case Some(json) =>
          job.getConfiguration.set("fs.gs.auth.service.account.json.keyfile", json)
        case None =>
          // Client id/secret of Google-managed project associated with the Cloud SDK
          job.getConfiguration.setBoolean("fs.gs.auth.service.account.enable", false)
          job.getConfiguration.set("fs.gs.auth.client.id", "32555940559.apps.googleusercontent.com")
          job.getConfiguration.set("fs.gs.auth.client.secret", "ZmssLNjJy2998hD4CTg2ejr2")
      }
    }

    def unsetCredentials(job: Job): Unit = {
      job.getConfiguration.unset("fs.gs.auth.service.account.json.keyfile")
      job.getConfiguration.unset("fs.gs.auth.service.account.enable")
      job.getConfiguration.unset("fs.gs.auth.client.id")
      job.getConfiguration.unset("fs.gs.auth.client.secret")
    }
  }

}
