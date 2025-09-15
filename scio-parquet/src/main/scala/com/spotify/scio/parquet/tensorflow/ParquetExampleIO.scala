/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.parquet.tensorflow

import com.spotify.parquet.tensorflow.{
  TensorflowExampleParquetInputFormat,
  TensorflowExampleParquetReader,
  TensorflowExampleReadSupport
}

import java.lang.{Boolean => JBoolean}
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.read.{ParquetRead, ParquetReadConfiguration, ReadSupportFactory}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetReader}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.proto.example.{Example, Features}
import org.tensorflow.metadata.v0.Schema

import scala.jdk.CollectionConverters._

final case class ParquetExampleIO(path: String) extends ScioIO[Example] {
  override type ReadP = ParquetExampleIO.ReadParam
  override type WriteP = ParquetExampleIO.WriteParam
  override val tapT: TapT.Aux[Example, Example] = TapOf[Example]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Example] = {
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
  ): SCollection[Example] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)

    Option(params.projection).foreach { projection =>
      TensorflowExampleReadSupport.setRequestedProjection(conf, projection)
      TensorflowExampleReadSupport.setExampleReadSchema(conf, projection)
    }

    Option(params.predicate).foreach { predicate =>
      ParquetInputFormat.setFilterPredicate(conf, predicate)
    }

    val coder = CoderMaterializer.beam(sc, Coder[Example])

    sc.applyTransform(
      ParquetRead.read(
        ReadSupportFactory.example,
        new SerializableConfiguration(conf),
        filePattern,
        identity[Example]
      )
    ).setCoder(coder)
  }

  private def readLegacy(
    sc: ScioContext,
    conf: Configuration,
    params: ReadP
  ): SCollection[Example] = {
    val job = Job.getInstance(conf)
    GcsConnectorUtil.setInputPaths(sc, job, path)
    job.setInputFormatClass(classOf[TensorflowExampleParquetInputFormat])
    job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
    job.getConfiguration.setClass("value.class", classOf[Example], classOf[Example])

    ParquetInputFormat.setReadSupportClass(job, classOf[TensorflowExampleReadSupport])
    Option(params.projection).foreach { projection =>
      TensorflowExampleParquetInputFormat.setRequestedProjection(job, projection)
      TensorflowExampleParquetInputFormat.setExampleReadSchema(job, projection)
    }

    Option(params.predicate).foreach { predicate =>
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, predicate)
    }

    val source = HadoopFormatIO
      .read[JBoolean, Example]()
      // Hadoop input always emit key-value, and `Void` causes NPE in Beam coder
      .withKeyTranslation(new SimpleFunction[Void, JBoolean]() {
        override def apply(input: Void): JBoolean = true
      })
      .withConfiguration(job.getConfiguration)
    sc.applyTransform(source).map(_.getValue)
  }

  override protected def readTest(sc: ScioContext, params: ReadP): SCollection[Example] = {
    // The projection function is not part of the test input, so it must be applied directly
    val projectionOpt = Option(params.projection)
    TestDataManager
      .getInput(sc.testId.get)(ParquetExampleIO(path))
      .toSCollection(sc)
      .map { example =>
        projectionOpt match {
          case None             => example
          case Some(projection) =>
            val featureNames = projection.getFeatureList.asScala.map(_.getName).toSet
            val projectedFeatures = example.getFeatures.getFeatureMap.asScala.filter {
              case (k, _) => featureNames.contains(k)
            }.asJava

            example.toBuilder
              .setFeatures(Features.newBuilder().putAllFeature(projectedFeatures))
              .build()
        }
      }
  }

  private def parquetExampleOut(
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
      .constant(fp, SerializableFunctions.identity[Example])
    val job = Job.getInstance(ParquetConfiguration.ofNullable(conf))
    if (isLocalRunner) GcsConnectorUtil.setCredentials(job)
    val sink = new ParquetExampleFileBasedSink(
      StaticValueProvider.of(tempDirectory),
      dynamicDestinations,
      schema,
      job.getConfiguration,
      compression
    )
    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  override protected def write(data: SCollection[Example], params: WriteP): Tap[Example] = {
    data.applyInternal(
      parquetExampleOut(
        path,
        params.schema,
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
    tap(ParquetExampleIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[Example] =
    ParquetExampleTap(path, params)
}

object ParquetExampleIO {

  object ReadParam {
    val DefaultProjection: Schema = null
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
    projection: Schema = ReadParam.DefaultProjection,
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
    schema: Schema,
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

final case class ParquetExampleTap(path: String, params: ParquetExampleIO.ReadParam)
    extends Tap[Example] {
  override def value: Iterator[Example] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val xs = FileSystems.`match`(filePattern).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader: ParquetReader[Example] = TensorflowExampleParquetReader
        .builder(BeamInputFile.of(metadata.resourceId()))
        .withConf(Option(params.conf).getOrElse(new Configuration()))
        .build()
      new Iterator[Example] {
        private var current: Example = reader.read()
        override def hasNext: Boolean = current != null
        override def next(): Example = {
          val r = current
          current = reader.read()
          r
        }
      }
    }
  }

  override def open(sc: ScioContext): SCollection[Example] =
    sc.read(ParquetExampleIO(path))(params)
}
