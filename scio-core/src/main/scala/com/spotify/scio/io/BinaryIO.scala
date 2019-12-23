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

import java.io.OutputStream
import java.nio.channels.{Channels, WritableByteChannel}

import com.spotify.scio.ScioContext
import com.spotify.scio.io.BinaryIO.BytesSink
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io._

/**
 * A ScioIO class for writing raw bytes to files.
 * Like TextIO, but without newline delimiters and operating over Array[Byte] instead of String.
 * @param path a path to write to.
 */
final case class BinaryIO(path: String) extends ScioIO[Array[Byte]] {
  override type ReadP = Nothing
  override type WriteP = BinaryIO.WriteParam
  final override val tapT = EmptyTapOf[Array[Byte]]

  override def testId: String = s"BinaryIO($path)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[Array[Byte]] =
    throw new UnsupportedOperationException("BinaryIO is write-only")

  override protected def write(data: SCollection[Array[Byte]], params: WriteP): Tap[Nothing] = {
    data.applyInternal(
      FileIO
        .write[Array[Byte]]
        .via(new BytesSink(params.header, params.footer, params.framePrefix, params.frameSuffix))
        .withCompression(params.compression)
        .withNumShards(params.numShards)
        .withPrefix(BinaryIO.WriteParam.DefaultPrefix)
        .withSuffix(params.suffix)
        .to(pathWithShards(path))
    )
    EmptyTap
  }

  override def tap(params: Nothing): Tap[Nothing] = EmptyTap

  private[scio] def pathWithShards(path: String) =
    path.replaceAll("\\/+$", "")
}

object BinaryIO {
  object WriteParam {
    private[scio] val DefaultPrefix = "part"
    private[scio] val DefaultSuffix = ".bin"
    private[scio] val DefaultNumShards = 0
    private[scio] val DefaultCompression = Compression.UNCOMPRESSED
    private[scio] val DefaultHeader = Array.emptyByteArray
    private[scio] val DefaultFooter = Array.emptyByteArray
    private[scio] val DefaultFramePrefix: Array[Byte] => Array[Byte] = _ => Array.emptyByteArray
    private[scio] val DefaultFrameSuffix: Array[Byte] => Array[Byte] = _ => Array.emptyByteArray
  }

  final case class WriteParam(
    suffix: String = WriteParam.DefaultSuffix,
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression,
    header: Array[Byte] = WriteParam.DefaultHeader,
    footer: Array[Byte] = WriteParam.DefaultFooter,
    framePrefix: Array[Byte] => Array[Byte] = WriteParam.DefaultFramePrefix,
    frameSuffix: Array[Byte] => Array[Byte] = WriteParam.DefaultFrameSuffix
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
