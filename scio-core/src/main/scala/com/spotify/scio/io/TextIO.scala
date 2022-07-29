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
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.util.ScioUtil.FilenamePolicyCreator
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.beam.sdk.io.{Compression, FileSystems, ShardNameTemplate, TextIO => BTextIO}
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.io.IOUtils

import java.nio.charset.StandardCharsets
import scala.jdk.CollectionConverters._
import scala.util.Try
import org.apache.beam.sdk.io.fs.ResourceId

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

  private def textOut(
    write: BTextIO.Write,
    path: String,
    suffix: String,
    numShards: Int,
    compression: Compression,
    header: Option[String],
    footer: Option[String],
    // TODO this should be in all the file IOs?
    shardNameTemplate: String,
    tempDirectory: ResourceId,
    filenamePolicyCreator: FilenamePolicyCreator,
    isWindowed: Boolean
  ) = {
    if(tempDirectory == null) throw new IllegalArgumentException("tempDirectory must not be null")
    if(shardNameTemplate != null && filenamePolicyCreator != null) throw new IllegalArgumentException("shardNameTemplate and filenamePolicyCreator may not be used together")

    val fp = Option(filenamePolicyCreator)
      .map(c => c.apply(ScioUtil.pathWithShards(path, ""), suffix, isWindowed))
      .getOrElse(ScioUtil.defaultFilenamePolicy(ScioUtil.pathWithShards(path), shardNameTemplate, suffix, isWindowed))

    var transform = write
      .to(fp)
      .withTempDirectory(tempDirectory)
//      .to(path.replaceAll("\\/+$", ""))
//      .withSuffix(params.suffix)
//      .withShardNameTemplate(params.shardNameTemplate)
      .withNumShards(numShards)
      .withCompression(compression)

    transform = header.fold(transform)(transform.withHeader)
    transform = footer.fold(transform)(transform.withFooter)

    if(!isWindowed) transform else transform.withWindowedWrites()
    //    transform = Option(params.tempDirectory)
//      .map(ScioUtil.toResourceId)
//      .fold(transform)(transform.withTempDirectory)
//    transform
  }

  override protected def write(data: SCollection[String], params: WriteP): Tap[String] = {
    data.applyInternal(
      textOut(
        BTextIO.write(),
        path,
        params.suffix,
        params.numShards,
        params.compression,
        params.header,
        params.footer,
        params.shardNameTemplate,
        ScioUtil.tempDirOrDefault(params.tempDirectory, data.context),
        params.filenamePolicyCreator,
        ScioUtil.isWindowed(data)
      )
    )
    tap(TextIO.ReadParam())
  }

  override def tap(params: ReadP): Tap[String] = {
    println(s"TAP path: $path -> ${ScioUtil.addPartSuffix(path)}")
    TextTap(ScioUtil.addPartSuffix(path))
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
    private[scio] val DefaultShardNameTemplate = null
    private[scio] val FallbackShardNameTemplate = "/part" + ShardNameTemplate.INDEX_OF_MAX
    private[scio] val DefaultTempDirectory = null
    private[scio] val DefaultFilenamePolicyCreator = null
  }
  final case class WriteParam(
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Option[String] = WriteParam.DefaultHeader,
    footer: Option[String] = WriteParam.DefaultFooter,
    shardNameTemplate: String = WriteParam.DefaultShardNameTemplate,
    tempDirectory: String = WriteParam.DefaultTempDirectory,
    filenamePolicyCreator: FilenamePolicyCreator = WriteParam.DefaultFilenamePolicyCreator
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
