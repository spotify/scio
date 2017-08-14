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

import java.lang.{Boolean => JBoolean}

import com.spotify.scio.ScioContext
import com.spotify.scio.testing.AvroIO
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.avro.AvroParquetInputFormat
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat

import scala.reflect.ClassTag

/**
 * Main package for Parquet Avro APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.parquet.avro._
 * }}}
 */
package object avro {

  /** Alias for [[me.lyh.parquet.avro.Projection]]. */
  val Projection = me.lyh.parquet.avro.Projection

  /** Alias for [[me.lyh.parquet.avro.Predicate]]. */
  val Predicate = me.lyh.parquet.avro.Predicate

  /** Enhanced version of [[ScioContext]] with Parquet Avro methods. */
  implicit class ParquetAvroScioContext(val self: ScioContext) extends AnyVal {

    /**
     * Get an SCollection for a Parquet file as Avro records.
     *
     * Note that due to limitations of the underlying [[HadoopInputFormatIO]],
     * Avro [[org.apache.avro.generic.GenericRecord GenericRecord]] and dynamic work rebalancing
     * are not supported. Without the latter, pipelines may not autoscale up or down during the
     * initial read and subsequent fused transforms.
     *
     * @group input
     */
    def parquetAvroFile[T <: SpecificRecordBase : ClassTag](path: String,
                                                            projection: Schema = null,
                                                            predicate: FilterPredicate = null)
    : SCollection[T] = self.requireNotClosed {
      if (self.isTest) {
        self.getTestInput(AvroIO[T](path))
      } else {
        val cls = ScioUtil.classOf[T]
        val readSchema = cls.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]

        val job = Job.getInstance()

        // Client id/secret of Google-managed project associated with the Cloud SDK
        // These are needed since `FileInputFormat.setInputPaths` validates paths locally and
        // requires the user's GCP credentials.
        job.getConfiguration.setBoolean("fs.gs.auth.service.account.enable", false)
        job.getConfiguration.set("fs.gs.auth.client.id", "32555940559.apps.googleusercontent.com")
        job.getConfiguration.set("fs.gs.auth.client.secret", "ZmssLNjJy2998hD4CTg2ejr2")

        FileInputFormat.setInputPaths(job, path)

        // These will interfere with credentials in Dataflow workers
        if (!ScioUtil.isLocalRunner(self.options.getRunner)) {
          job.getConfiguration.unset("fs.gs.auth.service.account.enable")
          job.getConfiguration.unset("fs.gs.auth.client.id")
          job.getConfiguration.unset("fs.gs.auth.client.secret")
        }

        job.setInputFormatClass(classOf[AvroParquetInputFormat[T]])
        job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
        job.getConfiguration.setClass("value.class", cls, cls)

        AvroParquetInputFormat.setAvroReadSchema(job, readSchema)
        if (projection != null) {
          AvroParquetInputFormat.setRequestedProjection(job, projection)
        }
        if (predicate != null) {
          ParquetInputFormat.setFilterPredicate(job.getConfiguration, predicate)
        }

        val source = HadoopInputFormatIO.read[JBoolean, T]()
          .withConfiguration(job.getConfiguration)
          .withKeyTranslation(new SimpleFunction[Void, JBoolean]() {
            override def apply(input: Void): JBoolean = true // workaround for NPE
          })
        self
          .wrap(self.applyInternal(source))
          .map(_.getValue)
      }
    }
  }

}
