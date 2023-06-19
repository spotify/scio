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

package com.spotify.scio.tensorflow

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.{ScioIO, Tap, TapOf}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{
  Compression,
  DynamicFileDestinations,
  TFRecordFileBasedSink,
  WriteFiles
}
import org.apache.beam.sdk.{io => beam}
import org.tensorflow.proto.example.{Example, SequenceExample}
import com.spotify.scio.io.TapT
import com.spotify.scio.util.FilenamePolicySupplier
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider
import org.apache.beam.sdk.transforms.SerializableFunctions

final case class TFRecordIO(path: String) extends ScioIO[Array[Byte]] {
  override type ReadP = TFRecordIO.ReadParam
  override type WriteP = TFRecordIO.WriteParam
  override val tapT: TapT.Aux[Array[Byte], Array[Byte]] = TapOf[Array[Byte]]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Array[Byte]] = {
    val coder = CoderMaterializer.beam(sc, Coder.arrayByteCoder)
    TFRecordMethods.read(sc, path, params).setCoder(coder)
  }

  override protected def write(data: SCollection[Array[Byte]], params: WriteP): Tap[Array[Byte]] = {
    TFRecordMethods.write(data, path, params)
    tap(TFRecordIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[Array[Byte]] =
    TFRecordMethods.tap(path, params)
}

object TFRecordIO {

  object ReadParam {
    val DefaultCompression: Compression = Compression.AUTO
    val DefaultSuffix: String = null

    private[scio] def apply(params: WriteParam): ReadParam =
      new ReadParam(
        compression = params.compression,
        suffix = params.suffix + params.compression.getSuggestedSuffix
      )
  }

  final case class ReadParam private (
    compression: Compression = ReadParam.DefaultCompression,
    suffix: String = ReadParam.DefaultSuffix
  )

  object WriteParam {
    val DefaultSuffix: String = ".tfrecords"
    val DefaultCompression: Compression = Compression.UNCOMPRESSED
    val DefaultNumShards: Int = 0
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
    val DefaultPrefix: String = null
    val DefaultShardNameTemplate: String = null
    val DefaultTempDirectory: String = null
  }

  final case class WriteParam private (
    suffix: String = WriteParam.DefaultSuffix,
    compression: Compression = WriteParam.DefaultCompression,
    numShards: Int = WriteParam.DefaultNumShards,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory
  )
}

final case class TFExampleIO(path: String) extends ScioIO[Example] {
  override type ReadP = TFExampleIO.ReadParam
  override type WriteP = TFExampleIO.WriteParam
  override val tapT: TapT.Aux[Example, Example] = TapOf[Example]

  override def testId: String = s"TFExampleIO($path)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Example] =
    TFRecordMethods.read(sc, path, params).map(Example.parseFrom)

  override protected def write(data: SCollection[Example], params: WriteP): Tap[Example] = {
    TFRecordMethods.write(data.map(_.toByteArray), path, params)
    tap(TFExampleIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[Example] =
    TFRecordMethods.tap(path, params).map(Example.parseFrom)
}

object TFExampleIO {
  type ReadParam = TFRecordIO.ReadParam
  val ReadParam = TFRecordIO.ReadParam
  type WriteParam = TFRecordIO.WriteParam
  val WriteParam = TFRecordIO.WriteParam
}

final case class TFSequenceExampleIO(path: String) extends ScioIO[SequenceExample] {
  override type ReadP = TFExampleIO.ReadParam
  override type WriteP = TFExampleIO.WriteParam
  override val tapT: TapT.Aux[SequenceExample, SequenceExample] = TapOf[SequenceExample]

  override def testId: String = s"TFSequenceExampleIO($path)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[SequenceExample] =
    TFRecordMethods.read(sc, path, params).map(SequenceExample.parseFrom)

  override protected def write(
    data: SCollection[SequenceExample],
    params: WriteP
  ): Tap[SequenceExample] = {
    TFRecordMethods.write(data.map(_.toByteArray), path, params)
    tap(
      TFExampleIO.ReadParam(
        params.compression,
        params.suffix + params.compression.getSuggestedSuffix
      )
    )
  }

  override def tap(params: ReadP): Tap[SequenceExample] =
    TFRecordMethods.tap(path, params).map(SequenceExample.parseFrom)
}

private object TFRecordMethods {
  def read(
    sc: ScioContext,
    path: String,
    params: TFRecordIO.ReadParam
  ): SCollection[Array[Byte]] = {
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    sc.applyTransform(
      beam.TFRecordIO
        .read()
        .from(filePattern)
        .withCompression(params.compression)
    )
  }

  private def tfWrite(
    path: String,
    suffix: String,
    numShards: Int,
    compression: Compression,
    filenamePolicySupplier: FilenamePolicySupplier,
    prefix: String,
    shardNameTemplate: String,
    isWindowed: Boolean,
    tempDirectory: ResourceId
  ) = {
    require(tempDirectory != null, "tempDirectory must not be null")
    val fp = FilenamePolicySupplier.resolve(
      filenamePolicySupplier = filenamePolicySupplier,
      prefix = prefix,
      shardNameTemplate = shardNameTemplate,
      isWindowed = isWindowed
    )(ScioUtil.strippedPath(path), suffix)

    val dynamicDestinations = DynamicFileDestinations
      .constant(fp, SerializableFunctions.identity[Array[Byte]])

    val sink = new TFRecordFileBasedSink(
      StaticValueProvider.of(tempDirectory),
      dynamicDestinations,
      compression
    )

    val transform = WriteFiles.to(sink).withNumShards(numShards)
    if (!isWindowed) transform else transform.withWindowedWrites()
  }

  def write(data: SCollection[Array[Byte]], path: String, params: TFRecordIO.WriteParam): Unit = {
    data.applyInternal(
      tfWrite(
        path,
        params.suffix,
        params.numShards,
        params.compression,
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context)
      )
    )

    ()
  }

  def tap(path: String, read: TFRecordIO.ReadParam): Tap[Array[Byte]] =
    TFRecordFileTap(path, read)
}
