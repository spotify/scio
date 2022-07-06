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

import java.io.{BufferedInputStream, InputStream, OutputStream}
import java.nio.channels.{Channels, WritableByteChannel}

import com.spotify.scio.ScioContext
import com.spotify.scio.io.BinaryIO.BytesSink
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io._
import org.apache.beam.sdk.io.FileIO.Write.FileNaming
import org.apache.beam.sdk.io.fs.MatchResult.Metadata
import org.apache.commons.compress.compressors.CompressorStreamFactory
import scala.jdk.CollectionConverters._

import scala.util.Try

/**
 * A ScioIO class for writing raw bytes to files. Like TextIO, but without newline delimiters and
 * operating over Array[Byte] instead of String.
 * @param path
 *   a path to write to.
 */
final case class BinaryIO(path: String) extends ScioIO[Array[Byte]] {
  override type ReadP = Nothing
  override type WriteP = BinaryIO.WriteParam
  override val tapT: TapT.Aux[Array[Byte], Nothing] = EmptyTapOf[Array[Byte]]

  override def testId: String = s"BinaryIO($path)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Array[Byte]] =
    throw new UnsupportedOperationException("BinaryIO is write-only")

  override protected def write(data: SCollection[Array[Byte]], params: WriteP): Tap[Nothing] = {
    var transform = FileIO
      .write[Array[Byte]]
      .via(new BytesSink(params.header, params.footer, params.framePrefix, params.frameSuffix))
      .withCompression(params.compression)
      .withNumShards(params.numShards)
      .to(pathWithShards(path))

    transform = params.fileNaming.fold {
      transform
        .withPrefix(params.prefix)
        .withSuffix(params.suffix)
    }(transform.withNaming)

    transform = Option(params.tempDirectory)
      .fold(transform)(transform.withTempDirectory)

    data.applyInternal(transform)
    EmptyTap
  }

  override def tap(params: Nothing): Tap[Nothing] = EmptyTap

  private def pathWithShards(path: String) = path.replaceAll("\\/+$", "")
}

object BinaryIO {

  private[scio] def openInputStreamsFor(path: String): Iterator[InputStream] = {
    val factory = new CompressorStreamFactory()

    def wrapInputStream(in: InputStream) = {
      val buffered = new BufferedInputStream(in)
      Try(factory.createCompressorInputStream(buffered)).getOrElse(buffered)
    }

    listFiles(path).map(getObjectInputStream).map(wrapInputStream).iterator
  }

  private def listFiles(path: String): Seq[Metadata] =
    FileSystems.`match`(path).metadata().iterator.asScala.toSeq

  private def getObjectInputStream(meta: Metadata): InputStream =
    Channels.newInputStream(FileSystems.open(meta.resourceId()))

  object WriteParam {
    private[scio] val DefaultFileNaming = Option.empty[FileNaming]
    private[scio] val DefaultPrefix = "part"
    private[scio] val DefaultSuffix = ".bin"
    private[scio] val DefaultNumShards = 0
    private[scio] val DefaultCompression = Compression.UNCOMPRESSED
    private[scio] val DefaultHeader = Array.emptyByteArray
    private[scio] val DefaultFooter = Array.emptyByteArray
    private[scio] val DefaultFramePrefix: Array[Byte] => Array[Byte] = _ => Array.emptyByteArray
    private[scio] val DefaultFrameSuffix: Array[Byte] => Array[Byte] = _ => Array.emptyByteArray
    private[scio] val DefaultTempDirectory = null
  }

  final case class WriteParam(
    prefix: String = WriteParam.DefaultPrefix,
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Array[Byte] = WriteParam.DefaultHeader,
    footer: Array[Byte] = WriteParam.DefaultFooter,
    framePrefix: Array[Byte] => Array[Byte] = WriteParam.DefaultFramePrefix,
    frameSuffix: Array[Byte] => Array[Byte] = WriteParam.DefaultFrameSuffix,
    fileNaming: Option[FileNaming] = WriteParam.DefaultFileNaming,
    tempDirectory: String = WriteParam.DefaultTempDirectory
  )

  final private class BytesSink(
    val header: Array[Byte],
    val footer: Array[Byte],
    val framePrefix: Array[Byte] => Array[Byte],
    val frameSuffix: Array[Byte] => Array[Byte]
  ) extends FileIO.Sink[Array[Byte]] {
    @transient private var channel: OutputStream = _

    override def open(channel: WritableByteChannel): Unit = {
      this.channel = Channels.newOutputStream(channel)
      this.channel.write(header)
    }

    override def flush(): Unit = {
      if (this.channel == null) {
        throw new IllegalStateException("Trying to flush a BytesSink that has not been opened")
      }

      this.channel.write(footer)
      this.channel.flush()
    }

    override def write(datum: Array[Byte]): Unit = {
      if (this.channel == null) {
        throw new IllegalStateException("Trying to write to a BytesSink that has not been opened")
      }

      this.channel.write(framePrefix(datum))
      this.channel.write(datum)
      this.channel.write(frameSuffix(datum))
    }
  }
}
