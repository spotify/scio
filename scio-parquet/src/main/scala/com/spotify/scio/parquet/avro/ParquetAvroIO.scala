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
import com.spotify.scio.parquet.{GcsConnectorUtil, ParquetConfiguration}
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
  AvroReadSupport,
  GenericDataSupplier
}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.jdk.CollectionConverters._
import scala.reflect.{classTag, ClassTag}

final case class ParquetAvroIO[T: ClassTag: Coder](path: String) extends ScioIO[T] {
  override type ReadP = ParquetAvroIO.ReadParam[_, T]
  override type WriteP = ParquetAvroIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val bCoder = CoderMaterializer.beam(sc, Coder[T])
    sc.pipeline.getCoderRegistry.registerCoderForClass(ScioUtil.classOf[T], bCoder)
    params.setupConfig()
    params.read(sc, path)(Coder[T])
  }

  override protected def readTest(sc: ScioContext, params: ReadP): SCollection[T] = {
    type AvroType = params.avroClass.type
    // The projection function is not part of the test input, so it must be applied directly
    TestDataManager
      .getInput(sc.testId.get)(ParquetAvroIO[AvroType](path)(classTag, null))
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
    isLocalRunner: Boolean,
    metadata: Map[String, String]
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
    val job = Job.getInstance(conf)
    if (isLocalRunner) GcsConnectorUtil.setCredentials(job)

    val sink = new ParquetAvroFileBasedSink[T](
      StaticValueProvider.of(tempDirectory),
      dynamicDestinations,
      schema,
      job.getConfiguration,
      compression,
      metadata.asJava
    )
    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    val avroClass = ScioUtil.classOf[T]
    val isSpecific: Boolean = classOf[SpecificRecord] isAssignableFrom avroClass
    val writerSchema = if (isSpecific) ReflectData.get().getSchema(avroClass) else params.schema

    data.applyInternal(
      parquetOut(
        path,
        writerSchema,
        params.suffix,
        params.numShards,
        params.compression,
        ParquetConfiguration.ofNullable(params.conf),
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        ScioUtil.isLocalRunner(data.context.options.getRunner),
        params.metadata
      )
    )
    tap(ParquetAvroIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[T] =
    ParquetAvroTap(path, params)
}

object ParquetAvroIO {
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
    lazy val confOrDefault = ParquetConfiguration.ofNullable(conf)
    val avroClass: Class[A] = ScioUtil.classOf[A]
    val isSpecific: Boolean = classOf[SpecificRecord] isAssignableFrom avroClass
    val readSchema: Schema =
      if (isSpecific) ReflectData.get().getSchema(avroClass) else projection

    def read(sc: ScioContext, path: String)(implicit coder: Coder[T]): SCollection[T] = {
      if (ParquetReadConfiguration.getUseSplittableDoFn(confOrDefault, sc.options)) {
        readSplittableDoFn(sc, path)
      } else {
        readLegacy(sc, path)
      }
    }

    def setupConfig(): Unit = {
      AvroReadSupport.setAvroReadSchema(confOrDefault, readSchema)
      AvroReadSupport.setRequestedProjection(
        confOrDefault,
        Option(projection).getOrElse(readSchema)
      )

      if (predicate != null) {
        ParquetInputFormat.setFilterPredicate(confOrDefault, predicate)
      }

      // Needed to make GenericRecord read by parquet-avro work with Beam's
      // org.apache.beam.sdk.extensions.avro.coders.AvroCoder
      if (!isSpecific) {
        confOrDefault.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false)
        if (confOrDefault.get(AvroReadSupport.AVRO_DATA_SUPPLIER) == null) {
          confOrDefault.setClass(
            AvroReadSupport.AVRO_DATA_SUPPLIER,
            classOf[GenericDataSupplier],
            classOf[AvroDataSupplier]
          )
        }
      }
    }

    private def readSplittableDoFn(sc: ScioContext, path: String)(implicit
      coder: Coder[T]
    ): SCollection[T] = {
      val filePattern = ScioUtil.filePattern(path, suffix)
      val bCoder = CoderMaterializer.beam(sc, coder)
      val cleanedProjectionFn = ClosureCleaner.clean(projectionFn)

      sc.applyTransform(
        ParquetRead.read[A, T](
          ReadSupportFactory.avro,
          new SerializableConfiguration(confOrDefault),
          filePattern,
          Functions.serializableFn(cleanedProjectionFn)
        )
      ).setCoder(bCoder)
    }

    private def readLegacy(sc: ScioContext, path: String)(implicit
      coder: Coder[T]
    ): SCollection[T] = {
      val job = Job.getInstance(confOrDefault)
      val filePattern = ScioUtil.filePattern(path, suffix)
      GcsConnectorUtil.setInputPaths(sc, job, filePattern)
      job.setInputFormatClass(classOf[AvroParquetInputFormat[T]])
      job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
      job.getConfiguration.setClass("value.class", avroClass, avroClass)

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
    val DefaultMetadata: Map[String, String] = null
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
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    metadata: Map[String, String] = WriteParam.DefaultMetadata
  )
}
