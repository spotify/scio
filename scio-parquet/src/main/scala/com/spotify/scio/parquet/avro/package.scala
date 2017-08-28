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
import com.spotify.scio.io.Tap
import com.spotify.scio.testing.AvroIO
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO
import org.apache.beam.sdk.io.{DefaultFilenamePolicy, FileBasedSink, HadoopWriteFiles}
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.avro.AvroParquetInputFormat
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat

import scala.concurrent.Future
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
        setInputPaths(job, path)
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

    private def setInputPaths(job: Job, path: String): Unit = {
      // This is needed since `FileInputFormat.setInputPaths` validates paths locally and requires
      // the user's GCP credentials.
      GcsConnectorUtil.setCredentials(job)

      FileInputFormat.setInputPaths(job, path)

      // It will interfere with credentials in Dataflow workers
      if (!ScioUtil.isLocalRunner(self.options.getRunner)) {
        GcsConnectorUtil.unsetCredentials(job)
      }
    }

  }

  /** Enhanced version of [[SCollection]] with Parquet Avro methods. */
  implicit class ParquetAvroSCollection[T : ClassTag]
  (val self: SCollection[T]) {
    /**
     * Save this SCollection of Avro records as a Parquet file.
     * @param schema must be not null if `T` is of type
     *               [[org.apache.avro.generic.GenericRecord GenericRecord]].
     */
    def saveAsParquetAvroFile(path: String,
                              numShards: Int = 0,
                              schema: Schema = null,
                              suffix: String = ""): Future[Tap[T]] = {
      if (self.context.isTest) {
        self.context.testOut(AvroIO(path))(self)
      } else {
        val job = Job.getInstance()
        if (ScioUtil.isLocalRunner(self.context.options.getRunner)) {
          GcsConnectorUtil.setCredentials(job)
        }

        val cls = ScioUtil.classOf[T]
        val writerSchema = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
          ReflectData.get().getSchema(cls)
        } else {
          schema
        }
        val resource = FileBasedSink.convertToFileResourceIfPossible(self.pathWithShards(path))
        val prefix = StaticValueProvider.of(resource)
        val policy = DefaultFilenamePolicy.constructUsingStandardParameters(
          prefix, null, ".parquet", false)
        val sink = new ParquetAvroSink[T](prefix, policy, writerSchema, job.getConfiguration)
        val t = HadoopWriteFiles.to(sink).withNumShards(numShards)
        self.applyInternal(t)
      }
      Future.failed(new NotImplementedError("Parquet Avro future not implemented"))
    }
  }

  private object GcsConnectorUtil {
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
