/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.parquet.avro

import java.lang.{Boolean => JBoolean}

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap}
import com.spotify.scio.util.{ClosureCleaner, ScioUtil}
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.hadoop.inputformat.HadoopInputFormatIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.avro.AvroParquetInputFormat
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.concurrent.Future
import scala.reflect.ClassTag

final case class ParquetAvroIO[T: ClassTag](path: String) extends ScioIO[T] {

  override type ReadP = ParquetAvroIO.ReadParam[_, T]
  override type WriteP = ParquetAvroIO.WriteParam

  private val cls = ScioUtil.classOf[T]

  override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val job = Job.getInstance()
    setInputPaths(sc, job, path)
    job.setInputFormatClass(classOf[AvroParquetInputFormat[T]])
    job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
    job.getConfiguration.setClass("value.class", params.avroClass, params.avroClass)

    AvroParquetInputFormat.setAvroReadSchema(job, params.readSchema)
    if (params.projection != null) {
      AvroParquetInputFormat.setRequestedProjection(job, params.projection)
    }
    if (params.predicate != null) {
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, params.predicate)
    }

    val source = params.read.withConfiguration(job.getConfiguration)
    sc.wrap(sc.applyInternal(source)).map(_.getValue)
  }

  override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
      val job = Job.getInstance()
      if (ScioUtil.isLocalRunner(data.context.options.getRunner)) {
        GcsConnectorUtil.setCredentials(job)
      }

      val writerSchema = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
        ReflectData.get().getSchema(cls)
      } else {
        params.schema
      }
      val resource = FileBasedSink.convertToFileResourceIfPossible(data.pathWithShards(path))
      val prefix = StaticValueProvider.of(resource)
      val usedFilenamePolicy = DefaultFilenamePolicy.fromStandardParameters(
        prefix, null, ".parquet", false)
      val destinations = DynamicFileDestinations.constant[T](usedFilenamePolicy)
      val sink = new ParquetAvroSink[T](
        prefix, destinations, writerSchema, job.getConfiguration, params.compression)
      val t = WriteFiles.to(sink).withNumShards(params.numShards)
      data.applyInternal(t)
      Future.failed(new NotImplementedError("Parquet Avro future not implemented"))
  }

  override def tap(params: ReadP): Tap[T] =
    throw new NotImplementedError("Parquet Avro tap not implemented")

  private def setInputPaths(sc: ScioContext, job: Job, path: String): Unit = {
    // This is needed since `FileInputFormat.setInputPaths` validates paths locally and requires
    // the user's GCP credentials.
    GcsConnectorUtil.setCredentials(job)

    FileInputFormat.setInputPaths(job, path)

    // It will interfere with credentials in Dataflow workers
    if (!ScioUtil.isLocalRunner(sc.options.getRunner)) {
      GcsConnectorUtil.unsetCredentials(job)
    }
  }
}

object ParquetAvroIO {
  final case class ReadParam[A: ClassTag, T: ClassTag](projection: Schema,
                                                       predicate: FilterPredicate,
                                                       projectionFn: A => T) {
    val avroClass: Class[A] = ScioUtil.classOf[A]
    val readSchema: Schema = avroClass.getMethod("getClassSchema").invoke(null).asInstanceOf[Schema]

    val read: HadoopInputFormatIO.Read[JBoolean, T] = {
      val g = ClosureCleaner(projectionFn)  // defeat closure
      val aCls = avroClass
      val oCls = ScioUtil.classOf[T]
      HadoopInputFormatIO.read[JBoolean, T]()
        .withKeyTranslation(new SimpleFunction[Void, JBoolean]() {
          override def apply(input: Void): JBoolean = true // workaround for NPE
        })
        .withValueTranslation(new SimpleFunction[A, T]() {
          // Workaround for incomplete Avro objects
          // `SCollection#map` might throw NPE on incomplete Avro objects when the runner tries
          // to serialized them. Lifting the mapping function here fixes the problem.
          override def apply(input: A): T = g(input)
          override def getInputTypeDescriptor = TypeDescriptor.of(aCls)
          override def getOutputTypeDescriptor = TypeDescriptor.of(oCls)
        })
    }
  }
  final case class WriteParam(numShards: Int = 0,
                              schema: Schema = null,
                              suffix: String = "",
                              compression: CompressionCodecName = CompressionCodecName.SNAPPY)
}
