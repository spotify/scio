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
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil}
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
import org.apache.beam.sdk.values.{TypeDescriptor, WindowingStrategy}
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetInputFormat, AvroParquetReader}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import com.spotify.scio.io.TapT
import org.apache.beam.sdk.io.FileBasedSink.FilenamePolicy
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.hadoop.conf.Configuration

import scala.util.Either

final case class ParquetAvroIO[T: ClassTag: Coder](path: String) extends ScioIO[T] {
  override type ReadP = ParquetAvroIO.ReadParam[_, T]
  override type WriteP = ParquetAvroIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  private val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    sc.pipeline.getCoderRegistry.registerCoderForClass(ScioUtil.classOf[T], coder)
    sc.applyTransform(params.read(sc, path)).map(_.getValue)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val job = Job.getInstance(params.conf)
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
    val fileNamePolicy =
      params.filenameFunction
        .map({
          case Left(f) =>
            createFilenamePolicy(resource, params.suffix, windowedFilenameFunction = f)
          case Right(f) => createFilenamePolicy(resource, params.suffix, filenameFunction = f)
        })
        .getOrElse(
          DefaultFilenamePolicy.fromStandardParameters(prefix, null, params.suffix, false)
        )
    val destinations = DynamicFileDestinations.constant[T](fileNamePolicy)
    val sink = new ParquetAvroSink[T](
      prefix,
      destinations,
      writerSchema,
      job.getConfiguration,
      params.compression
    )
    val t =
      if (data.internal.getWindowingStrategy != WindowingStrategy.globalDefault()) {
        WriteFiles.to(sink).withNumShards(params.numShards).withWindowedWrites()
      } else {
        WriteFiles.to(sink).withNumShards(params.numShards)
      }
    data.applyInternal(t)
    tap(ParquetAvroIO.ReadParam[T, T](identity[T] _, writerSchema, null))
  }

  override def tap(params: ReadP): Tap[T] =
    ParquetAvroTap(ScioUtil.addPartSuffix(path), params)

  def createFilenamePolicy(
    baseFileName: ResourceId,
    filenameSuffix: String,
    windowedFilenameFunction: (Int, Int, BoundedWindow, PaneInfo) => String = (_, _, _, _) =>
      throw new NotImplementedError(
        "saveAsDynamicParquetAvroFile for windowed SCollections requires a windowed filename function"
      ),
    filenameFunction: (Int, Int) => String = (_, _) =>
      throw new NotImplementedError("saveAsDynamicParquetAvroFile requires a filename function")
  ): FilenamePolicy = {

    new FileBasedSink.FilenamePolicy {
      override def windowedFilename(
        shardNumber: Int,
        numShards: Int,
        window: BoundedWindow,
        paneInfo: PaneInfo,
        outputFileHints: FileBasedSink.OutputFileHints
      ): ResourceId = {
        val filename = windowedFilenameFunction(shardNumber, numShards, window, paneInfo)
        baseFileName.getCurrentDirectory.resolve(
          filename + filenameSuffix,
          StandardResolveOptions.RESOLVE_FILE
        )
      }

      override def unwindowedFilename(
        shardNumber: Int,
        numShards: Int,
        outputFileHints: FileBasedSink.OutputFileHints
      ): ResourceId = {
        val filename = filenameFunction(shardNumber, numShards)
        baseFileName.getCurrentDirectory.resolve(
          filename + filenameSuffix,
          StandardResolveOptions.RESOLVE_FILE
        )
      }
    }

  }

}

object ParquetAvroIO {
  object ReadParam {
    private[avro] val DefaultProjection = null
    private[avro] val DefaultPredicate = null
    private[avro] val DefaultConfiguration = new Configuration()

    @deprecated(
      "Use ReadParam(projectionFn, projection, predicate, conf) instead",
      since = "0.10.0"
    )
    def apply[A: ClassTag, T: ClassTag](
      projection: Schema,
      predicate: FilterPredicate,
      projectionFn: A => T
    ): ReadParam[A, T] =
      ReadParam(projectionFn, projection, predicate)
  }

  final case class ReadParam[A: ClassTag, T: ClassTag] private[avro] (
    projectionFn: A => T,
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration
  ) {
    val avroClass: Class[A] = ScioUtil.classOf[A]
    val readSchema: Schema = {
      if (classOf[SpecificRecordBase] isAssignableFrom avroClass) {
        ReflectData.get().getSchema(avroClass)
      } else {
        projection
      }
    }

    def read(sc: ScioContext, path: String): HadoopFormatIO.Read[JBoolean, T] = {
      val job = Job.getInstance(conf)
      GcsConnectorUtil.setInputPaths(sc, job, path)
      job.setInputFormatClass(classOf[AvroParquetInputFormat[T]])
      job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
      job.getConfiguration.setClass("value.class", avroClass, avroClass)

      AvroParquetInputFormat.setAvroReadSchema(job, readSchema)
      if (projection != null) {
        AvroParquetInputFormat.setRequestedProjection(job, projection)
      }
      if (predicate != null) {
        ParquetInputFormat.setFilterPredicate(job.getConfiguration, predicate)
      }

      val g = ClosureCleaner.clean(projectionFn) // defeat closure
      val aCls = avroClass
      val oCls = ScioUtil.classOf[T]
      HadoopFormatIO
        .read[JBoolean, T]()
        // Hadoop input always emit key-value, and `Void` causes NPE in Beam coder
        .withKeyTranslation(new SimpleFunction[Void, JBoolean]() {
          override def apply(input: Void): JBoolean = true
        })
        .withValueTranslation(new SimpleFunction[A, T]() {
          // Workaround for incomplete Avro objects
          // `SCollection#map` might throw NPE on incomplete Avro objects when the runner tries
          // to serialized them. Lifting the mapping function here fixes the problem.
          override def apply(input: A): T = g(input)
          override def getInputTypeDescriptor = TypeDescriptor.of(aCls)
          override def getOutputTypeDescriptor = TypeDescriptor.of(oCls)
        })
        .withConfiguration(job.getConfiguration)
    }
  }

  object WriteParam {
    private[avro] val DefaultSchema = null
    private[avro] val DefaultNumShards = 0
    private[avro] val DefaultSuffix = ".parquet"
    private[avro] val DefaultCompression = CompressionCodecName.GZIP
    private[avro] val DefaultFilenameFunction = None
    private[avro] val DefaultConfiguration = new Configuration()
  }

  final case class WriteParam private[avro] (
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    filenameFunction: Option[
      Either[(Int, Int, BoundedWindow, PaneInfo) => String, (Int, Int) => String]
    ] = WriteParam.DefaultFilenameFunction,
    conf: Configuration = WriteParam.DefaultConfiguration
  )
}

case class ParquetAvroTap[A, T: ClassTag: Coder](
  path: String,
  params: ParquetAvroIO.ReadParam[A, T]
) extends Tap[T] {
  override def value: Iterator[T] = {
    val xs = FileSystems.`match`(path).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader = AvroParquetReader
        .builder[A](BeamInputFile.of(metadata.resourceId()))
        .withConf(params.conf)
        .build()
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
