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
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.parquet.read.{ParquetRead, ParquetReadConfiguration, ReadSupportFactory}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil, ParquetConfiguration}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.{FilenamePolicySupplier, Functions, ScioUtil}
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{
  AvroDataSupplier,
  AvroParquetInputFormat,
  AvroParquetReader,
  AvroReadSupport,
  AvroWriteSupport,
  GenericDataSupplier
}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

final case class ParquetAvroIO[T: ClassTag: Coder](path: String) extends ScioIO[T] {
  override type ReadP = ParquetAvroIO.ReadParam[_, T]
  override type WriteP = ParquetAvroIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  private val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val bCoder = CoderMaterializer.beam(sc, Coder[T])
    sc.pipeline.getCoderRegistry.registerCoderForClass(ScioUtil.classOf[T], bCoder)
    params.read(sc, path)(Coder[T])
  }

  override protected def readTest(sc: ScioContext, params: ReadP): SCollection[T] = {
    type AvroType = params.avroClass.type

    // The projection function is not part of the test input, so it must be applied directly
    TestDataManager
      .getInput(sc.testId.get)(ParquetAvroIO[AvroType](path))
      .toSCollection(sc)
      .map(params.projectionFn.asInstanceOf[AvroType => T])
  }

  private def parquetOut(
    path: String,
    schema: Schema,
    suffix: String,
    numShards: Int,
    compression: CompressionCodecName,
    conf: Configuration,
    filenamePolicySupplier: FilenamePolicySupplier,
    prefix: String,
    shardNameTemplate: String,
    isWindowed: Boolean,
    tempDirectory: ResourceId,
    isLocalRunner: Boolean
  ) = {
    require(tempDirectory != null, "tempDirectory must not be null")
    val fp = FilenamePolicySupplier.resolve(
      filenamePolicySupplier = filenamePolicySupplier,
      prefix = prefix,
      shardNameTemplate = shardNameTemplate,
      isWindowed = isWindowed
    )(ScioUtil.strippedPath(path), suffix)
    val dynamicDestinations = DynamicFileDestinations
      .constant(fp, SerializableFunctions.identity[T])
    val job = Job.getInstance(ParquetConfiguration.ofNullable(conf))
    if (isLocalRunner) GcsConnectorUtil.setCredentials(job)

    val sink = new ParquetAvroFileBasedSink[T](
      StaticValueProvider.of(tempDirectory),
      dynamicDestinations,
      schema,
      job.getConfiguration,
      compression
    )
    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val isAssignable = classOf[SpecificRecord].isAssignableFrom(cls)
    val writerSchema = if (isAssignable) ReflectData.get().getSchema(cls) else params.schema
    val conf = ParquetConfiguration.ofNullable(params.conf)
    if (
      conf.get(AvroWriteSupport.AVRO_DATA_SUPPLIER) == null && ParquetAvroIO.containsLogicalType(
        writerSchema
      )
    ) {
      ParquetAvroIO.log.warn(
        s"Detected a logical type in schema `$writerSchema`, but Configuration key `${AvroWriteSupport.AVRO_DATA_SUPPLIER}`" +
          s"was not set to a logical type supplier. See https://spotify.github.io/scio/io/Parquet.html#logical-types for more information."
      )
    }

    data.applyInternal(
      parquetOut(
        path,
        writerSchema,
        params.suffix,
        params.numShards,
        params.compression,
        conf,
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        ScioUtil.isLocalRunner(data.context.options.getRunner)
      )
    )
    tap(ParquetAvroIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[T] =
    ParquetAvroTap(path, params)
}

object ParquetAvroIO {
  private lazy val log = LoggerFactory.getLogger(getClass)

  object ReadParam {
    val DefaultProjection: Schema = null
    val DefaultPredicate: FilterPredicate = null
    val DefaultConfiguration: Configuration = null
    val DefaultSuffix: String = null

    private[scio] def apply[T: ClassTag](params: WriteParam): ReadParam[T, T] =
      new ReadParam[T, T](
        projectionFn = identity,
        projection = params.schema,
        conf = params.conf,
        suffix = params.suffix
      )
  }

  final case class ReadParam[A: ClassTag, T: ClassTag] private (
    projectionFn: A => T,
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = null
  ) {
    val avroClass: Class[A] = ScioUtil.classOf[A]
    val isSpecific: Boolean = classOf[SpecificRecord] isAssignableFrom avroClass
    val readSchema: Schema =
      if (isSpecific) ReflectData.get().getSchema(avroClass) else projection

    def read(sc: ScioContext, path: String)(implicit coder: Coder[T]): SCollection[T] = {
      val jobConf = ParquetConfiguration.ofNullable(conf)

      if (
        jobConf.get(AvroReadSupport.AVRO_DATA_SUPPLIER) == null && ParquetAvroIO
          .containsLogicalType(
            readSchema
          )
      ) {
        log.warn(
          s"Detected a logical type in schema `$readSchema`, but Configuration key `${AvroReadSupport.AVRO_DATA_SUPPLIER}`" +
            s"was not set to a logical type supplier. See https://spotify.github.io/scio/io/Parquet.html#logical-types for more information."
        )
      }

      // Needed to make GenericRecord read by parquet-avro work with Beam's
      // org.apache.beam.sdk.extensions.avro.coders.AvroCoder
      if (!isSpecific) {
        jobConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false)

        if (jobConf.get(AvroReadSupport.AVRO_DATA_SUPPLIER) == null) {
          jobConf.setClass(
            AvroReadSupport.AVRO_DATA_SUPPLIER,
            classOf[GenericDataSupplier],
            classOf[AvroDataSupplier]
          )
        }
      }

      if (ParquetReadConfiguration.getUseSplittableDoFn(jobConf, sc.options)) {
        readSplittableDoFn(sc, jobConf, path)
      } else {
        readLegacy(sc, jobConf, path)
      }
    }

    private def readSplittableDoFn(sc: ScioContext, conf: Configuration, path: String)(implicit
      coder: Coder[T]
    ): SCollection[T] = {
      AvroReadSupport.setAvroReadSchema(conf, readSchema)
      if (projection != null) {
        AvroReadSupport.setRequestedProjection(conf, projection)
      }
      if (predicate != null) {
        ParquetInputFormat.setFilterPredicate(conf, predicate)
      }

      val bCoder = CoderMaterializer.beam(sc, coder)
      val cleanedProjectionFn = ClosureCleaner.clean(projectionFn)

      sc.applyTransform(
        ParquetRead.read[A, T](
          ReadSupportFactory.avro,
          new SerializableConfiguration(conf),
          path,
          Functions.serializableFn(cleanedProjectionFn)
        )
      ).setCoder(bCoder)
    }

    private def readLegacy(sc: ScioContext, conf: Configuration, path: String)(implicit
      coder: Coder[T]
    ): SCollection[T] = {
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
      val transform = HadoopFormatIO
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

      sc.applyTransform(transform).map(_.getValue)
    }
  }

  private[avro] def containsLogicalType(s: Schema): Boolean = {
    s.getLogicalType != null || (s.getType match {
      case Schema.Type.RECORD => s.getFields.asScala.exists(f => containsLogicalType(f.schema()))
      case Schema.Type.ARRAY  => containsLogicalType(s.getElementType)
      case Schema.Type.UNION  => s.getTypes.asScala.exists(t => containsLogicalType(t))
      case Schema.Type.MAP    => containsLogicalType(s.getValueType)
      case _                  => false
    })
  }

  object WriteParam {
    val DefaultSchema: Schema = null
    val DefaultNumShards: Int = 0
    val DefaultSuffix: String = ".parquet"
    val DefaultCompression: CompressionCodecName = CompressionCodecName.ZSTD
    val DefaultConfiguration: Configuration = null
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
    val DefaultPrefix: String = null
    val DefaultShardNameTemplate: String = null
    val DefaultTempDirectory: String = null
  }

  final case class WriteParam private (
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory
  )
}

final case class ParquetAvroTap[A, T: ClassTag: Coder](
  path: String,
  params: ParquetAvroIO.ReadParam[A, T]
) extends Tap[T] {
  override def value: Iterator[T] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val xs = FileSystems.`match`(filePattern).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader = AvroParquetReader
        .builder[A](BeamInputFile.of(metadata.resourceId()))
        .withConf(ParquetConfiguration.ofNullable(params.conf))
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
