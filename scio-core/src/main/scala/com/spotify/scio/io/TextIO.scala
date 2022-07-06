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

import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.beam.sdk.io.{Compression, FileSystems, ShardNameTemplate, TextIO => BTextIO}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import java.io.{BufferedInputStream, InputStream, SequenceInputStream}
import java.nio.channels.Channels
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.jdk.CollectionConverters._
import scala.util.Try

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
    data.applyInternal(textOut(path, params))
    tap(TextIO.ReadParam())
  }

  override def tap(params: ReadP): Tap[String] =
    TextTap(ScioUtil.addPartSuffix(path))

  private def textOut(path: String, params: WriteP) = {
    var transform = BTextIO
      .write()
      .to(path.replaceAll("\\/+$", ""))
      .withSuffix(params.suffix)
      .withShardNameTemplate(params.shardNameTemplate)
      .withNumShards(params.numShards)
      .withCompression(params.compression)

    transform = params.header.fold(transform)(transform.withHeader)
    transform = params.footer.fold(transform)(transform.withFooter)
    transform = Option(params.tempDirectory)
      .map(ScioUtil.toResourceId)
      .fold(transform)(transform.withTempDirectory)

    transform
  }

}

object TextIO {
  final case class ReadParam(compression: Compression = Compression.AUTO)

  object WriteParam {
    private[scio] val DefaultHeader = Option.empty[String]
    private[scio] val DefaultFooter = Option.empty[String]
    private[scio] val DefaultSuffix = ".txt"
    private[scio] val DefaultNumShards = 0
    private[scio] val DefaultCompression = Compression.UNCOMPRESSED
    private[scio] val DefaultShardNameTemplate = "/part" + ShardNameTemplate.INDEX_OF_MAX
    private[scio] val DefaultTempDirectory = null
  }
  final case class WriteParam(
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Option[String] = WriteParam.DefaultHeader,
    footer: Option[String] = WriteParam.DefaultFooter,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory
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
