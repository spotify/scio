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

package com.spotify.scio.parquet.avro

import java.lang.{Boolean => JBoolean}
import java.nio.channels.SeekableByteChannel

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf}
import com.spotify.scio.parquet.{BeamParquetInputFile, GcsConnectorUtil}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroParquetReader}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

final case class ParquetAvroIO[T: ClassTag: Coder](path: String) extends ScioIO[T] {
  override type ReadP = ParquetAvroIO.ReadParam[_, T]
  override type WriteP = ParquetAvroIO.WriteParam
  override val tapT = TapOf[T]

  private val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val job = Job.getInstance()
    GcsConnectorUtil.setInputPaths(sc, job, path)
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

    val coder = CoderMaterializer.beam(sc, Coder[T])
    sc.pipeline.getCoderRegistry.registerCoderForClass(ScioUtil.classOf[T], coder)

    val source = params.read.withConfiguration(job.getConfiguration)
    sc.wrap(sc.applyInternal(source)).map(_.getValue)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val job = Job.getInstance()
    if (ScioUtil.isLocalRunner(data.context.options.getRunner)) {
      GcsConnectorUtil.setCredentials(job)
    }

    val writerSchema = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
      ReflectData.get().getSchema(cls)
    } else {
      params.schema
    }
    val resource =
      FileBasedSink.convertToFileResourceIfPossible(ScioUtil.pathWithShards(path))
    val prefix = StaticValueProvider.of(resource)
    val usedFilenamePolicy =
      DefaultFilenamePolicy.fromStandardParameters(prefix, null, params.suffix, false)
    val destinations = DynamicFileDestinations.constant[T](usedFilenamePolicy)
    val sink = new ParquetAvroSink[T](
      prefix,
      destinations,
      writerSchema,
      job.getConfiguration,
      params.compression
    )
    val t = WriteFiles.to(sink).withNumShards(params.numShards)
    data.applyInternal(t)
    tap(ParquetAvroIO.ReadParam[T, T](writerSchema, null, identity))
  }

  override def tap(params: ReadP): Tap[T] =
    ParquetAvroTap(ScioUtil.addPartSuffix(path), params)
}

object ParquetAvroIO {
  final case class ReadParam[A: ClassTag, T: ClassTag] private (
    projection: Schema,
    predicate: FilterPredicate,
    projectionFn: A => T
  ) {
    val avroClass: Class[A] = ScioUtil.classOf[A]
    val readSchema: Schema = {
      if (classOf[SpecificRecordBase] isAssignableFrom avroClass) {
        ReflectData.get().getSchema(avroClass)
      } else {
        projection
      }
    }

    val read: HadoopFormatIO.Read[JBoolean, T] = {
      val g = ClosureCleaner.clean(projectionFn) // defeat closure
      val aCls = avroClass
      val oCls = ScioUtil.classOf[T]
      HadoopFormatIO
        .read[JBoolean, T]()
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

  object WriteParam {
    private[avro] val DefaultSchema = null
    private[avro] val DefaultNumShards = 0
    private[avro] val DefaultSuffix = ".parquet"
    private[avro] val DefaultCompression = CompressionCodecName.SNAPPY
  }

  final case class WriteParam private (
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression
  )
}

case class ParquetAvroTap[A, T: ClassTag: Coder](
  path: String,
  params: ParquetAvroIO.ReadParam[A, T]
) extends Tap[T] {
  override def value: Iterator[T] = {
    val xs = FileSystems.`match`(path).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val channel = FileSystems
        .open(metadata.resourceId())
        .asInstanceOf[SeekableByteChannel]
      val reader =
        AvroParquetReader.builder[A](new BeamParquetInputFile(channel)).build()
      new Iterator[T] {
        private var current: A = reader.read()
        override def hasNext: Boolean = current != null
        override def next(): T = {
          val r = params.projectionFn(current)
          current = reader.read()
          r
        }
      }
    }
  }
  override def open(sc: ScioContext): SCollection[T] =
    sc.read(ParquetAvroIO[T](path))(params)
}
