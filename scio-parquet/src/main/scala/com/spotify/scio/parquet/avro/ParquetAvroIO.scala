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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.parquet.read.{ParquetRead, ReadSupportFactory}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.{FilenamePolicyCreator, Functions, ScioUtil}
import com.spotify.scio.values.SCollection
import com.twitter.chill.ClosureCleaner
import org.apache.avro.Schema
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.avro.{AvroParquetReader, AvroReadSupport, GenericDataSupplier}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

final case class ParquetAvroIO[T: ClassTag: Coder](path: String) extends ScioIO[T] {
  override type ReadP = ParquetAvroIO.ReadParam[_, T]
  override type WriteP = ParquetAvroIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  private val cls = ScioUtil.classOf[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    sc.pipeline.getCoderRegistry.registerCoderForClass(ScioUtil.classOf[T], coder)
    params.read(sc, path)
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
    shardNameTemplate: String,
    tempDirectory: ResourceId,
    filenamePolicyCreator: FilenamePolicyCreator,
    isWindowed: Boolean,
    isLocalRunner: Boolean
  ) = {
    if (tempDirectory == null) throw new IllegalArgumentException("tempDirectory must not be null")
    if (shardNameTemplate != null && filenamePolicyCreator != null)
      throw new IllegalArgumentException(
        "shardNameTemplate and filenamePolicyCreator may not be used together"
      )

    val fp = Option(filenamePolicyCreator)
      .map(c => c.apply(ScioUtil.pathWithPrefix(path, ""), suffix))
      .getOrElse(
        ScioUtil.defaultFilenamePolicy(
          ScioUtil.pathWithPrefix(path),
          shardNameTemplate,
          suffix,
          isWindowed
        )
      )

    val dynamicDestinations =
      DynamicFileDestinations.constant(fp, SerializableFunctions.identity[T])
    val job = Job.getInstance(conf)
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
    val isAssignable = classOf[SpecificRecordBase].isAssignableFrom(cls)
    val writerSchema = if (isAssignable) ReflectData.get().getSchema(cls) else params.schema

    data.applyInternal(
      parquetOut(
        path,
        writerSchema,
        params.suffix,
        params.numShards,
        params.compression,
        params.conf,
        params.shardNameTemplate,
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        params.filenamePolicyCreator,
        ScioUtil.isWindowed(data),
        ScioUtil.isLocalRunner(data.context.options.getRunner)
      )
    )
    tap(ParquetAvroIO.ReadParam[T, T](identity[T] _, writerSchema, null))
  }

  override def tap(params: ReadP): Tap[T] =
    ParquetAvroTap(ScioUtil.addPartSuffix(path), params)
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

  final case class ReadParam[A: ClassTag, T: ClassTag] private (
    projectionFn: A => T,
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration
  ) {
    val avroClass: Class[A] = ScioUtil.classOf[A]
    val isSpecific: Boolean = classOf[SpecificRecordBase] isAssignableFrom avroClass
    val readSchema: Schema =
      if (isSpecific) ReflectData.get().getSchema(avroClass) else projection

    def read(sc: ScioContext, path: String): SCollection[T] = {
      val hadoopConf = new Configuration(conf)
      // Needed to make GenericRecord read by parquet-avro work with Beam's
      // org.apache.beam.sdk.coders.AvroCoder.
      if (!isSpecific) {
        hadoopConf.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false)
        hadoopConf.set(
          AvroReadSupport.AVRO_DATA_SUPPLIER,
          classOf[GenericDataSupplier].getCanonicalName
        )
      }

      AvroReadSupport.setAvroReadSchema(hadoopConf, readSchema)
      if (projection != null) {
        AvroReadSupport.setRequestedProjection(hadoopConf, projection)
      }
      if (predicate != null) {
        ParquetInputFormat.setFilterPredicate(hadoopConf, predicate)
      }

      val tCoder = sc.pipeline.getCoderRegistry.getCoder(ScioUtil.classOf[T])
      val cleanedProjectionFn = ClosureCleaner.clean(projectionFn)

      sc.applyTransform(
        ParquetRead.read[A, T](
          ReadSupportFactory.avro,
          new SerializableConfiguration(hadoopConf),
          path,
          Functions.serializableFn(cleanedProjectionFn)
        )
      ).setCoder(tCoder)
    }
  }

  object WriteParam {
    private[scio] val DefaultSchema = null
    private[scio] val DefaultNumShards = 0
    private[scio] val DefaultSuffix = ".parquet"
    private[scio] val DefaultCompression = CompressionCodecName.GZIP
    private[scio] val DefaultConfiguration = new Configuration()
    private[scio] val DefaultShardNameTemplate = null
    private[scio] val DefaultTempDirectory = null
    private[scio] val DefaultFilenamePolicyCreator = null
  }

  final case class WriteParam private (
    schema: Schema = WriteParam.DefaultSchema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicyCreator: FilenamePolicyCreator = WriteParam.DefaultFilenamePolicyCreator
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
