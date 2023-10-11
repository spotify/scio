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

package com.spotify.scio.parquet.types

import java.lang.{Boolean => JBoolean}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.parquet.read.{ParquetRead, ParquetReadConfiguration, ReadSupportFactory}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil, ParquetConfiguration}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import magnolify.parquet.ParquetType
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.io.{DynamicFileDestinations, FileSystems, WriteFiles}
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

final case class ParquetTypeIO[T: ClassTag: Coder: ParquetType](
  path: String
) extends ScioIO[T] {
  override type ReadP = ParquetTypeIO.ReadParam
  override type WriteP = ParquetTypeIO.WriteParam
  override val tapT: TapT.Aux[T, T] = TapOf[T]

  private val tpe: ParquetType[T] = implicitly[ParquetType[T]]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val conf = ParquetConfiguration.ofNullable(params.conf)

    if (ParquetReadConfiguration.getUseSplittableDoFn(conf, sc.options)) {
      readSplittableDoFn(sc, conf, params)
    } else {
      readLegacy(sc, conf, params)
    }
  }

  private def readSplittableDoFn(
    sc: ScioContext,
    conf: Configuration,
    params: ReadP
  ): SCollection[T] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    if (params.predicate != null) {
      ParquetInputFormat.setFilterPredicate(conf, params.predicate)
    }

    val coder = CoderMaterializer.beam(sc, implicitly[Coder[T]])

    sc.applyTransform(
      ParquetRead.read(
        ReadSupportFactory.typed,
        new SerializableConfiguration(conf),
        filePattern,
        identity[T]
      )
    ).setCoder(coder)
  }

  private def readLegacy(sc: ScioContext, conf: Configuration, params: ReadP): SCollection[T] = {
    val cls = ScioUtil.classOf[T]
    val job = Job.getInstance(conf)
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    GcsConnectorUtil.setInputPaths(sc, job, filePattern)
    tpe.setupInput(job)
    job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
    job.getConfiguration.setClass("value.class", cls, cls)

    if (params.predicate != null) {
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, params.predicate)
    }

    val source = HadoopFormatIO
      .read[JBoolean, T]
      // Hadoop input always emit key-value, and `Void` causes NPE in Beam coder
      .withKeyTranslation(new SimpleFunction[Void, JBoolean]() {
        override def apply(input: Void): JBoolean = true
      })
      .withValueTranslation(
        new SimpleFunction[T, T]() {
          override def apply(input: T): T = input
          override def getInputTypeDescriptor: TypeDescriptor[T] = TypeDescriptor.of(cls)
        },
        CoderMaterializer.beam(sc, Coder[T])
      )
      .withConfiguration(job.getConfiguration)
      .withSkipValueClone(job.getConfiguration.getBoolean(ParquetReadConfiguration.SkipClone, true))
    sc.applyTransform(source).map(_.getValue)
  }

  private def parquetOut(
    path: String,
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
    val sink = new ParquetTypeFileBasedSink[T](
      StaticValueProvider.of(tempDirectory),
      dynamicDestinations,
      tpe,
      job.getConfiguration,
      compression
    )
    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    data.applyInternal(
      parquetOut(
        path,
        params.suffix,
        params.numShards,
        params.compression,
        params.conf,
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        ScioUtil.isLocalRunner(data.context.options.getRunner)
      )
    )
    tap(ParquetTypeIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[tapT.T] =
    ParquetTypeTap(path, params)
}

object ParquetTypeIO {

  object ReadParam {
    val DefaultPredicate: FilterPredicate = null
    val DefaultConfiguration: Configuration = null
    val DefaultSuffix: String = null

    private[scio] def apply(params: WriteParam): ReadParam =
      new ReadParam(
        conf = params.conf,
        suffix = params.suffix
      )
  }
  final case class ReadParam private (
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = ReadParam.DefaultSuffix
  )

  object WriteParam {
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

final case class ParquetTypeTap[T: ClassTag: Coder: ParquetType](
  path: String,
  params: ParquetTypeIO.ReadParam
) extends Tap[T] {
  override def value: Iterator[T] = {
    val tpe = implicitly[ParquetType[T]]
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val xs = FileSystems.`match`(filePattern).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader = tpe.readBuilder(BeamInputFile.of(metadata.resourceId())).build()
      new Iterator[T] {
        private var current: T = reader.read()
        override def hasNext: Boolean = current != null
        override def next(): T = {
          val r = current
          current = reader.read()
          r
        }
      }
    }
  }

  override def open(sc: ScioContext): SCollection[T] =
    sc.read(ParquetTypeIO[T](path))(params)
}
