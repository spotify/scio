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

package com.spotify.scio.io

import java.io.{BufferedInputStream, InputStream, SequenceInputStream}
import java.nio.channels.Channels
import java.util.Collections
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.beam.sdk.io.{Compression, FileSystems}
import org.apache.beam.sdk.{io => beam}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.util.chaining._
import org.apache.beam.sdk.io.fs.{EmptyMatchTreatment, ResourceId}

final case class TextIO(path: String) extends ScioIO[String] {
  override type ReadP = TextIO.ReadParam
  override type WriteP = TextIO.WriteParam
  final override val tapT: TapT.Aux[String, String] = TapOf[String]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[String] = {
    val coder = CoderMaterializer.beam(sc, Coder.stringCoder)
    val filePattern = ScioUtil.filePattern(path, params.suffix)
    val t = beam.TextIO
      .read()
      .from(filePattern)
      .withCompression(params.compression)
      .withEmptyMatchTreatment(params.emptyMatchTreatment)

    sc.applyTransform(t)
      .setCoder(coder)
  }

  private def textOut(
    path: String,
    suffix: String,
    numShards: Int,
    compression: Compression,
    header: Option[String],
    footer: Option[String],
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

    beam.TextIO
      .write()
      .to(fp)
      .withTempDirectory(tempDirectory)
      .withNumShards(numShards)
      .withCompression(compression)
      .pipe(w => header.fold(w)(w.withHeader))
      .pipe(w => footer.fold(w)(w.withFooter))
      .pipe(w => if (!isWindowed) w else w.withWindowedWrites())

  }

  override protected def write(data: SCollection[String], params: WriteP): Tap[String] = {
    data.applyInternal(
      textOut(
        path,
        params.suffix,
        params.numShards,
        params.compression,
        params.header,
        params.footer,
        params.filenamePolicySupplier,
        params.prefix,
        params.shardNameTemplate,
        ScioUtil.isWindowed(data),
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context)
      )
    )
    tap(TextIO.ReadParam(params))
  }

  override def tap(params: ReadP): Tap[String] =
    TextTap(path, params)
}

object TextIO {

  object ReadParam {
    val DefaultCompression: Compression = Compression.AUTO
    val DefaultEmptyMatchTreatment: EmptyMatchTreatment = EmptyMatchTreatment.DISALLOW
    val DefaultSuffix: String = null

    private[scio] def apply(params: WriteParam): ReadParam =
      new ReadParam(
        compression = params.compression,
        suffix = params.suffix + params.compression.getSuggestedSuffix
      )
  }

  final case class ReadParam private (
    compression: Compression = ReadParam.DefaultCompression,
    emptyMatchTreatment: EmptyMatchTreatment = ReadParam.DefaultEmptyMatchTreatment,
    suffix: String = ReadParam.DefaultSuffix
  )

  object WriteParam {
    val DefaultHeader: Option[String] = None
    val DefaultFooter: Option[String] = None
    val DefaultSuffix: String = ".txt"
    val DefaultNumShards: Int = 0
    val DefaultCompression: Compression = Compression.UNCOMPRESSED
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier = null
    val DefaultPrefix: String = null
    val DefaultShardNameTemplate: String = null
    val DefaultTempDirectory: String = null
  }

  val DefaultWriteParam: WriteParam = WriteParam()

  final case class WriteParam private (
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Option[String] = WriteParam.DefaultHeader,
    footer: Option[String] = WriteParam.DefaultFooter,
    filenamePolicySupplier: FilenamePolicySupplier = WriteParam.DefaultFilenamePolicySupplier,
    prefix: String = WriteParam.DefaultPrefix,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory
  )

  private[scio] def textFile(pattern: String): Iterator[String] = {
    val factory = new CompressorStreamFactory()

    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      Try(factory.createCompressorInputStream(buffered)).getOrElse(buffered)
    }

    val input = getDirectoryInputStream(pattern, wrapInputStream)
    IOUtils.lineIterator(input, StandardCharsets.UTF_8).asScala
  }

  private def getDirectoryInputStream(
    pattern: String,
    wrapperFn: InputStream => InputStream
  ): InputStream = {
    val inputs = listFiles(pattern).map(getObjectInputStream).map(wrapperFn).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }

  private def listFiles(pattern: String): Seq[Metadata] =
    FileSystems.`match`(pattern).metadata().iterator().asScala.toSeq

  private def getObjectInputStream(meta: Metadata): InputStream =
    Channels.newInputStream(FileSystems.open(meta.resourceId()))
}
