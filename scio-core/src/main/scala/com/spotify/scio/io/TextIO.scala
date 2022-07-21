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
import com.google.api.client.util.Charsets
import com.google.common.base.Preconditions
import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.ScioUtil.{BoundedFilenameFunction, UnboundedFilenameFunction, dynamicDestinations}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.beam.sdk.io.{Compression, FileIO, FileSystems, ShardNameTemplate, TextIO => BTextIO}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import java.io.{BufferedInputStream, InputStream, SequenceInputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.util.Try
import org.apache.beam.sdk.io.ShardNameTemplate
import org.apache.beam.sdk.transforms.Contextful
import org.apache.beam.sdk.transforms.windowing.{BoundedWindow, PaneInfo}
import org.apache.beam.sdk.values.WindowingStrategy

final case class TextIO(path: String) extends ScioIO[String] {
  override type ReadP = TextIO.ReadParam
  override type WriteP = TextIO.WriteParam
  final override val tapT: TapT.Aux[String, String] = TapOf[String]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[String] =
    sc.applyTransform(
      BTextIO
        .read()
        .from(path)
        .withCompression(params.compression)
    )

  override protected def write(data: SCollection[String], params: WriteP): Tap[String] = {
    val cleansedPath = path.replaceAll("\\/+$", "")
    val isWindowed = data.internal.getWindowingStrategy != WindowingStrategy.globalDefault()
    val shardTemplateProvided = params.shardNameTemplate != null
    val fnProvided = params.unboundedFilenameFunction != null || params.boundedFilenameFunction != null

    val xform = if(fnProvided) {
      var sink = BTextIO.sink()
      sink = params.header.fold(sink)(sink.withHeader)
      sink = params.footer.fold(sink)(sink.withFooter)

      val (_, destinations) = ScioUtil.dynamicDestinations[String](
        path,
        params.suffix,
        isWindowed,
        params.boundedFilenameFunction,
        params.unboundedFilenameFunction
      )

//      val destinationFn: Contextful[Contextful.Fn[String, String]] = Contextful.fn[String, String](
//
//      )
      val transform = FileIO.write()
//        .by(destinationFn)
        .withNumShards(params.numShards)
        .withCompression(params.compression)
        .withNaming(
          new FileIO.Write.FileNaming {
            override def getFilename(window: BoundedWindow, pane: PaneInfo, numShards: Int, shardIndex: Int, compression: Compression): String = ???
          }
        )
        .via(sink)
        .to(cleansedPath)

      // FileIO supports windowing by default
      Preconditions.checkArgument(
        !shardTemplateProvided,
        "shardNameTemplate may not be used when unboundedFilenameFunction or boundedFilenameFunction are provided", Nil: _*
      )

      transform
    } else {
      var transform = BTextIO.write()
        .withNumShards(params.numShards)
        .withCompression(params.compression)
      transform = params.header.fold(transform)(transform.withHeader)
      transform = params.footer.fold(transform)(transform.withFooter)
      transform = Option(params.tempDirectory)
        .map(ScioUtil.toResourceId)
        .fold(transform)(transform.withTempDirectory)
      // TextIO does _not_ support windowing by default
      if(isWindowed) transform = transform.withWindowedWrites()

      val template = if(shardTemplateProvided) params.shardNameTemplate else TextIO.WriteParam.FallbackShardNameTemplate
      transform = transform
        .to(cleansedPath)
        .withSuffix(params.suffix)
        .withShardNameTemplate(template)

      transform
    }

    data.applyInternal(xform)
    tap(TextIO.ReadParam())
  }

  override def tap(params: ReadP): Tap[String] =
    TextTap(ScioUtil.addPartSuffix(path))

}

object TextIO {
  final case class ReadParam(compression: Compression = Compression.AUTO)

  object WriteParam {
    private[scio] val DefaultHeader = Option.empty[String]
    private[scio] val DefaultFooter = Option.empty[String]
    private[scio] val DefaultSuffix = ".txt"
    private[scio] val DefaultNumShards = 0
    private[scio] val DefaultCompression = Compression.UNCOMPRESSED
    private[scio] val DefaultShardNameTemplate = null
    private[scio] val FallbackShardNameTemplate = "/part" + ShardNameTemplate.INDEX_OF_MAX
    private[scio] val DefaultTempDirectory = null
    private[scio] val DefaultBoundedFilenameFunction = null
    private[scio] val DefaultUnboundedFilenameFunction = null
  }
  final case class WriteParam(
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Option[String] = WriteParam.DefaultHeader,
    footer: Option[String] = WriteParam.DefaultFooter,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    boundedFilenameFunction: BoundedFilenameFunction = WriteParam.DefaultBoundedFilenameFunction,
    unboundedFilenameFunction: UnboundedFilenameFunction = WriteParam.DefaultUnboundedFilenameFunction
  )

  private[scio] def textFile(path: String): Iterator[String] = {
    val factory = new CompressorStreamFactory()

    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      Try(factory.createCompressorInputStream(buffered)).getOrElse(buffered)
    }

    val input = getDirectoryInputStream(path, wrapInputStream)
    IOUtils.lineIterator(input, StandardCharsets.UTF_8).asScala
  }

  private def getDirectoryInputStream(
    path: String,
    wrapperFn: InputStream => InputStream
  ): InputStream = {
    val inputs = listFiles(path).map(getObjectInputStream).map(wrapperFn).asJava
    new SequenceInputStream(Collections.enumeration(inputs))
  }

  private def listFiles(path: String): Seq[Metadata] =
    FileSystems.`match`(path).metadata().iterator().asScala.toSeq

  private def getObjectInputStream(meta: Metadata): InputStream =
    Channels.newInputStream(FileSystems.open(meta.resourceId()))
}
