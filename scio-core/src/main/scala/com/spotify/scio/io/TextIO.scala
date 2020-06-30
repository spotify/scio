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
import com.spotify.scio.ScioContext
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.beam.sdk.io.{
  Compression,
  FileBasedSink,
  FileIO => BFileIO,
  FileSystems,
  ShardNameTemplate,
  TextIO => BTextIO
}
import org.apache.beam.sdk.transforms.Create
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import scala.jdk.CollectionConverters._
import scala.util.Try

final case class TextIO(path: String) extends ScioIO[String] {
  override type ReadP = TextIO.ReadParam
  override type WriteP = TextIO.WriteParam
  final override val tapT = TapOf[String]

  override def testId: String = s"TextIO($path)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[String] =
    sc.wrap(
      sc.applyInternal(
        BTextIO
          .read()
          .from(path)
          .withCompression(params.compression)
      )
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
      .withWritableByteChannelFactory(
        FileBasedSink.CompressionType.fromCanonical(params.compression)
      )

    transform = params.header.fold(transform)(transform.withHeader)
    transform = params.header.fold(transform)(transform.withFooter)

    transform
  }

}

/** Read multiple files/file-patterns with beam TextIO readFiles API */
final case class TextReadFilesIO(paths: Iterable[String]) extends ScioIO[String] {
  override type ReadP = TextIO.ReadParam
  override type WriteP = Nothing
  override val tapT = TapOf[String]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[String] = {
    sc.wrap(
      sc.applyInternal(
        Create.of(paths.asJava)
      ).apply(BFileIO.matchAll())
        .apply(
          BFileIO
            .readMatches()
            .withCompression(params.compression)
        )
        .apply(BTextIO.readFiles())
    )
  }

  override protected def write(scoll: SCollection[String], params: WriteP): Tap[String] =
    throw new UnsupportedOperationException("TextReadFilesIO is read-only")

  override def tap(read: ReadP): Tap[String] =
    TextReadFilesTap(paths)

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
  }
  final case class WriteParam(
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Option[String] = WriteParam.DefaultHeader,
    footer: Option[String] = WriteParam.DefaultFooter,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate
  )

  private[scio] def textFile(path: String): Iterator[String] = {
    val factory = new CompressorStreamFactory()

    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      Try(factory.createCompressorInputStream(buffered)).getOrElse(buffered)
    }

    val input = getDirectoryInputStream(path, wrapInputStream)
    IOUtils.lineIterator(input, Charsets.UTF_8).asScala
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
