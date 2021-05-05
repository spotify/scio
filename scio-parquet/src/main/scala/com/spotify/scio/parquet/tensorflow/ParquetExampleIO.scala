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

import java.lang.{Boolean => JBoolean}
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{ScioIO, Tap, TapOf}
import com.spotify.scio.parquet.{BeamInputFile, GcsConnectorUtil}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import me.lyh.parquet.tensorflow.{
  ExampleParquetInputFormat,
  ExampleParquetReader,
  ExampleReadSupport,
  Schema
}
import org.apache.beam.sdk.io.{
  DefaultFilenamePolicy,
  DynamicFileDestinations,
  FileBasedSink,
  FileSystems,
  WriteFiles
}
import org.apache.beam.sdk.io.hadoop.format.HadoopFormatIO
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.hadoop.mapreduce.Job
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.tensorflow.proto.example.Example

import scala.jdk.CollectionConverters._
import com.spotify.scio.io.TapT
import org.apache.hadoop.conf.Configuration

final case class ParquetExampleIO(path: String) extends ScioIO[Example] {
  override type ReadP = ParquetExampleIO.ReadParam
  override type WriteP = ParquetExampleIO.WriteParam
  override val tapT: TapT.Aux[Example, Example] = TapOf[Example]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Example] = {
    val job = Job.getInstance(params.conf)
    GcsConnectorUtil.setInputPaths(sc, job, path)
    job.setInputFormatClass(classOf[ExampleParquetInputFormat])
    job.getConfiguration.setClass("key.class", classOf[Void], classOf[Void])
    job.getConfiguration.setClass("value.class", classOf[Example], classOf[Example])

    ParquetInputFormat.setReadSupportClass(job, classOf[ExampleReadSupport])
    if (params.projection != null) {
      ExampleParquetInputFormat.setFields(job, params.projection.asJava)
    }
    if (params.predicate != null) {
      ParquetInputFormat.setFilterPredicate(job.getConfiguration, params.predicate)
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

  override protected def write(data: SCollection[Example], params: WriteP): Tap[Example] = {
    val job = Job.getInstance(params.conf)
    if (ScioUtil.isLocalRunner(data.context.options.getRunner)) {
      GcsConnectorUtil.setCredentials(job)
    }

    val resource =
      FileBasedSink.convertToFileResourceIfPossible(ScioUtil.pathWithShards(path))
    val prefix = StaticValueProvider.of(resource)
    val usedFilenamePolicy =
      DefaultFilenamePolicy.fromStandardParameters(prefix, null, params.suffix, false)
    val destinations = DynamicFileDestinations.constant[Example](usedFilenamePolicy)
    val sink = new ParquetExampleSink(
      prefix,
      destinations,
      params.schema,
      job.getConfiguration,
      params.compression
    )
    val t = WriteFiles.to(sink).withNumShards(params.numShards)
    data.applyInternal(t)
    tap(ParquetExampleIO.ReadParam())
  }

  override def tap(params: ReadP): Tap[Example] =
    ParquetExampleTap(ScioUtil.addPartSuffix(path), params)
}

object ParquetExampleIO {
  object ReadParam {
    private[tensorflow] val DefaultProjection = null
    private[tensorflow] val DefaultPredicate = null
    private[tensorflow] val DefaultConfiguration = new Configuration()
  }
  final case class ReadParam private[tensorflow] (
    projection: Seq[String] = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration
  )

  object WriteParam {
    private[tensorflow] val DefaultNumShards = 0
    private[tensorflow] val DefaultSuffix = ".parquet"
    private[tensorflow] val DefaultCompression = CompressionCodecName.GZIP
    private[tensorflow] val DefaultConfiguration = new Configuration()
  }

  final case class WriteParam private[tensorflow] (
    schema: Schema,
    numShards: Int = WriteParam.DefaultNumShards,
    suffix: String = WriteParam.DefaultSuffix,
    compression: CompressionCodecName = WriteParam.DefaultCompression,
    conf: Configuration = WriteParam.DefaultConfiguration
  )
}

final case class ParquetExampleTap(path: String, params: ParquetExampleIO.ReadParam)
    extends Tap[Example] {
  override def value: Iterator[Example] = {
    val xs = FileSystems.`match`(path).metadata().asScala.toList
    xs.iterator.flatMap { metadata =>
      val reader = ExampleParquetReader
        .builder(BeamInputFile.of(metadata.resourceId()))
        .withConf(params.conf)
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
