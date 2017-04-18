/*
 * Copyright 2017 Spotify AB.
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

import java.io.{InputStream, OutputStream, PushbackInputStream}
import java.nio.channels.{Channels, ReadableByteChannel, WritableByteChannel}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.GZIPInputStream

import com.google.common.hash.Hashing
import com.google.common.primitives.Ints
import org.apache.beam.sdk.coders.{ByteArrayCoder, Coder}
import org.apache.beam.sdk.io.FileBasedSink.{FileBasedWriteOperation, FileBasedWriter}
import org.apache.beam.sdk.io.FileBasedSource.FileBasedReader
import org.apache.beam.sdk.io.{FileBasedSink, FileBasedSource}
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.util.MimeTypes
import org.apache.commons.compress.compressors.deflate._
import org.apache.commons.compress.compressors.gzip._

/** TensorFlow TFRecord options. */
object TFRecordOptions {

  /** Compression type. */
  sealed trait CompressionType
  object CompressionType {
    case object AUTO extends CompressionType
    case object NONE extends CompressionType
    case object ZLIB extends CompressionType
    case object GZIP extends CompressionType
  }

  /** Default read options. */
  val readDefault = TFRecordOptions(CompressionType.AUTO)

  /** Default write options. */
  val writeDefault = TFRecordOptions(CompressionType.NONE)

}

case class TFRecordOptions(compressionType: TFRecordOptions.CompressionType)

// =======================================================================
// Source
// =======================================================================

// workaround for multiple super class constructors
private[scio] object TFRecordSource {
  def apply(fileOrPatternSpec: String, opts: TFRecordOptions): TFRecordSource =
    new FileBasedSource[Array[Byte]](fileOrPatternSpec, 1L) with TFRecordSource {
      override val options: TFRecordOptions = opts
    }
  def apply(fileOrPatternSpec: String, opts: TFRecordOptions,
            startOffset: Long, endOffset: Long): TFRecordSource =
    new FileBasedSource[Array[Byte]](fileOrPatternSpec, 1L, startOffset, endOffset)
      with TFRecordSource {
      override val options: TFRecordOptions = opts
    }
}

private[scio] trait TFRecordSource extends FileBasedSource[Array[Byte]] {

  val options: TFRecordOptions

  override def createSingleFileReader(options: PipelineOptions): FileBasedReader[Array[Byte]] =
    new TFRecordReader(this)

  override def createForSubrangeOfFile(fileName: String,
                                       start: Long,
                                       end: Long): FileBasedSource[Array[Byte]] = {
    require(start == 0, "TFRecordSource is not splittable")
    // workaround for compression offset inconsistency: always read entire file
    TFRecordSource(fileName, options, 0, Long.MaxValue)
  }

  override def getDefaultOutputCoder: Coder[Array[Byte]] = ByteArrayCoder.of()
  override def isSplittable: Boolean = false

}

private class TFRecordReader(source: TFRecordSource) extends FileBasedReader[Array[Byte]](source) {

  private var inputStream: InputStream = _
  private var data: Array[Byte] = _
  private var count: Long = 0  // number of records

  override def readNextRecord(): Boolean = {
    data = TFRecordCodec.read(inputStream)
    if (data != null) {
      count += 1
      true
    } else {
      false
    }
  }

  override def startReading(channel: ReadableByteChannel): Unit = {
    val stream = Channels.newInputStream(channel)
    val options = source.asInstanceOf[TFRecordSource].options
    inputStream = TFRecordCodec.wrapInputStream(stream, options)
  }

  // workaround for compression offset inconsistency: number of records to approximate offset
  override def getCurrentOffset: Long = count
  override def getCurrent: Array[Byte] = data

}

// =======================================================================
// Sink
// =======================================================================

private[scio] class TFRecordSink(baseOutputFilename: String,
                                 extension: String,
                                 private[io] val options: TFRecordOptions)
  extends FileBasedSink[Array[Byte]](baseOutputFilename, extension) {

  require(
    options.compressionType != TFRecordOptions.CompressionType.AUTO,
    "Unsupported compression type AUTO")

  def this(baseOutputFilename: String, options: TFRecordOptions) =
    this(baseOutputFilename, ".tfrecords", options)

  override def createWriteOperation(options: PipelineOptions)
  : FileBasedWriteOperation[Array[Byte]] = new TFRecordWriteOperation(this)

}

private class TFRecordWriteOperation(sink: TFRecordSink)
  extends FileBasedWriteOperation[Array[Byte]](sink) {
  override def createWriter(options: PipelineOptions): FileBasedWriter[Array[Byte]] =
    new TFRecordWriter(this)
}

private class TFRecordWriter(writeOperation: TFRecordWriteOperation)
  extends FileBasedWriter[Array[Byte]](writeOperation) {

  mimeType = MimeTypes.BINARY

  private var outputStream: OutputStream = _

  override def prepareWrite(channel: WritableByteChannel): Unit = {
    import TFRecordOptions.CompressionType._

    val stream = Channels.newOutputStream(channel)
    val options = writeOperation.getSink.asInstanceOf[TFRecordSink].options
    outputStream = options.compressionType match {
      case AUTO => throw new RuntimeException("Unsupported compression type AUTO")
      case NONE => stream
      case ZLIB => new DeflateCompressorOutputStream(stream)
      case GZIP => new GzipCompressorOutputStream(stream)
    }
  }

  override def write(value: Array[Byte]): Unit = TFRecordCodec.write(outputStream, value)

  override def writeFooter(): Unit = {
    outputStream match {
      case s: DeflateCompressorOutputStream => s.finish()
      case s: GzipCompressorOutputStream => s.finish()
      case _ => Unit
    }
  }

}

// =======================================================================
// Codec
// =======================================================================

private object TFRecordCodec {

  private val headerLength: Int =
    (java.lang.Long.SIZE + java.lang.Integer.SIZE) / java.lang.Byte.SIZE
  private val footerLength: Int = java.lang.Integer.SIZE / java.lang.Byte.SIZE
  private val crc32c = Hashing.crc32c()

  private def mask(crc: Int): Int = ((crc >>> 15) | (crc << 17)) + 0xa282ead8

  def read(input: InputStream): Array[Byte] = {
    val headerBytes = readFully(input, headerLength)
    if (headerBytes != null) {
      val headerBuf = ByteBuffer.wrap(headerBytes).order(ByteOrder.LITTLE_ENDIAN)
      val length = headerBuf.getLong
      val maskedCrc32OfLength = headerBuf.getInt
      require(hashLong(length) == maskedCrc32OfLength, "Invalid masked CRC32 of length")

      val data = readFully(input, length.toInt)

      val footerBytes = readFully(input, footerLength)
      val footerBuf = ByteBuffer.wrap(footerBytes).order(ByteOrder.LITTLE_ENDIAN)
      val maskedCrc32OfData = footerBuf.getInt
      require(hashBytes(data) == maskedCrc32OfData, "Invalid masked CRC32 of data")
      data
    } else {
      null
    }
  }

  // InflaterInputStream#read may not fill a buffer fully even when there are more data available
  private def readFully(input: InputStream, length: Int): Array[Byte] = {
    val data = Array.ofDim[Byte](length)
    var n = 0
    var off = 0
    do {
      n = input.read(data, off, data.length - off)
      if (n > 0) {
        off += n
      }
    } while (n > 0 && off < data.length)
    if (n <= 0) null else data
  }

  def write(output: OutputStream, data: Array[Byte]): Unit = {
    val length = data.length.toLong
    val maskedCrc32OfLength = hashLong(length)
    val maskedCrc32OfData = hashBytes(data)

    val headerBuf = ByteBuffer.allocate(headerLength).order(ByteOrder.LITTLE_ENDIAN)
    headerBuf.putLong(length)
    headerBuf.putInt(maskedCrc32OfLength)
    output.write(headerBuf.array(), 0, headerLength)

    output.write(data)

    val footerBuf = ByteBuffer.allocate(footerLength).order(ByteOrder.LITTLE_ENDIAN)
    footerBuf.putInt(maskedCrc32OfData)
    output.write(footerBuf.array())
  }

  def wrapInputStream(stream: InputStream, options: TFRecordOptions): InputStream = {
    import TFRecordOptions.CompressionType._

    val deflateParam = new DeflateParameters()
    deflateParam.setWithZlibHeader(true)

    options.compressionType match {
      case AUTO =>
        val pushback = new PushbackInputStream(stream, 2)
        if (isInflaterInputStream(pushback)) {
          new DeflateCompressorInputStream(pushback, deflateParam)
        } else if (isGzipInputStream(pushback)) {
          new GzipCompressorInputStream(pushback)
        } else {
          pushback
        }
      case NONE => stream
      case ZLIB => new DeflateCompressorInputStream(stream, deflateParam)
      case GZIP => new GzipCompressorInputStream(stream)
    }
  }

  private def hashLong(x: Long): Int = mask(crc32c.hashLong(x).asInt())
  private def hashBytes(x: Array[Byte]): Int = mask(crc32c.hashBytes(x).asInt())

  private def isGzipInputStream(pushback: PushbackInputStream): Boolean = {
    val headerBytes = Array.ofDim[Byte](2)
    pushback.read(headerBytes)
    val zero: Byte = 0x00
    val header = Ints.fromBytes(zero, zero, headerBytes(1), headerBytes(0))
    pushback.unread(headerBytes)
    header == GZIPInputStream.GZIP_MAGIC
  }

  private def isInflaterInputStream(pushback: PushbackInputStream): Boolean = {
    val b1 = pushback.read()
    val b2 = pushback.read()
    pushback.unread(b2)
    pushback.unread(b1)
    b1 == 0x78 && (b1 * 256 + b2) % 31 == 0
  }

}
