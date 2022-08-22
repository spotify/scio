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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TapT}
import com.spotify.scio.parquet.read.{ParquetRead, ReadSupportFactory}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil}
import com.spotify.scio.testing.TestDataManager
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.{ExampleParquetInputFormat, ExampleParquetReader, Schema}
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SerializableFunctions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.proto.example.{Example, Features}

import scala.jdk.CollectionConverters._

final case class ParquetExampleIO(path: String) extends ScioIO[Example] {
  override type ReadP = ParquetExampleIO.ReadParam
  override type WriteP = ParquetExampleIO.WriteParam
  override val tapT: TapT.Aux[Example, Example] = TapOf[Example]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Example] = {
    val conf = Option(params.conf).getOrElse(new Configuration())
    val job = Job.getInstance(conf)

    Option(params.projection).foreach { projection =>
      ExampleParquetInputFormat.setFields(job, projection.asJava)
      conf.set(ExampleParquetInputFormat.FIELDS_KEY, String.join(",", projection: _*))
    }

    Option(params.predicate).foreach { predicate =>
      ParquetInputFormat.setFilterPredicate(conf, predicate)
    }

    val coder = CoderMaterializer.beam(sc, Coder[Example])

    sc.applyTransform(
      ParquetRead.read(
        ReadSupportFactory.example,
        new SerializableConfiguration(conf),
        path,
        identity[Example]
      )
    ).setCoder(coder)
  }

  override protected def readTest(sc: ScioContext, params: ReadP): SCollection[Example] = {
    // The projection function is not part of the test input, so it must be applied directly
    val projectionOpt = Option(params.projection)
    TestDataManager
      .getInput(sc.testId.get)(ParquetExampleIO(path))
      .toSCollection(sc)
      .map { case (example: Example) =>
        projectionOpt match {
          case None => example
          case Some(projection) =>
            val projectedFeatures = example.getFeatures.getFeatureMap.asScala.filter {
              case (k, _) => projection.contains(k)
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
    shardNameTemplate: String,
    tempDirectory: ResourceId,
    filenamePolicySupplier: FilenamePolicySupplier,
    isWindowed: Boolean,
    isLocalRunner: Boolean
  ) = {
    val fp = FilenamePolicySupplier.resolve(
      path,
      suffix,
      shardNameTemplate,
      tempDirectory,
      filenamePolicySupplier,
      isWindowed
    )
    val dynamicDestinations =
      DynamicFileDestinations.constant(fp, SerializableFunctions.identity[Example])
    val job = Job.getInstance(Option(conf).getOrElse(new Configuration()))
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
        params.shardNameTemplate,
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        params.filenamePolicySupplier,
        ScioUtil.isWindowed(data),
        ScioUtil.isLocalRunner(data.context.options.getRunner)
      )
    )
    tap(ParquetExampleIO.ReadParam())
  }

  override def tap(params: ReadP): Tap[Example] =
    ParquetExampleTap(ScioUtil.addPartSuffix(path), params)
}

object ParquetExampleIO {
  object ReadParam {
    private[tensorflow] val DefaultProjection = null
    private[tensorflow] val DefaultPredicate = null
    private[tensorflow] val DefaultConfiguration = null
  }
  final case class ReadParam private (
    projection: Seq[String] = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration
  )

  object WriteParam {
    private[tensorflow] val DefaultNumShards = 0
    private[tensorflow] val DefaultSuffix = ".parquet"
    private[tensorflow] val DefaultCompression = CompressionCodecName.GZIP
    private[tensorflow] val DefaultConfiguration = null
    private[tensorflow] val DefaultShardNameTemplate = null
    private[tensorflow] val DefaultTempDirectory = null
    private[tensorflow] val DefaultFilenamePolicySupplier = null
  }

  final case class WriteParam private (
    schema: Schema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier
  )
}

final case class ParquetExampleTap(path: String, params: ParquetExampleIO.ReadParam)
    extends Tap[Example] {
  override def value: Iterator[Example] = {
    val xs = FileSystems.`match`(path).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader = ExampleParquetReader
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

  override def open(sc: ScioContext): SCollection[Example] = sc.read(ParquetExampleIO(path))(params)
}
