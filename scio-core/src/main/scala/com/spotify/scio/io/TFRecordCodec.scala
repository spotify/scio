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

import java.io.{InputStream, PushbackInputStream}
import java.nio.{ByteBuffer, ByteOrder}
import java.util.zip.GZIPInputStream

import com.google.common.hash.Hashing
import com.google.common.primitives.Ints
import org.apache.beam.sdk.io.TFRecordIO.CompressionType
import org.apache.beam.sdk.repackaged.org.apache.commons.compress.compressors.deflate._
import org.apache.commons.compress.compressors.gzip._

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

  def wrapInputStream(stream: InputStream, compressionType: CompressionType): InputStream = {
    val deflateParam = new DeflateParameters()
    deflateParam.setWithZlibHeader(true)

    compressionType match {
      case CompressionType.AUTO =>
        val pushback = new PushbackInputStream(stream, 2)
        if (isInflaterInputStream(pushback)) {
          new DeflateCompressorInputStream(pushback, deflateParam)
        } else if (isGzipInputStream(pushback)) {
          new GzipCompressorInputStream(pushback)
        } else {
          pushback
        }
      case CompressionType.NONE => stream
      case CompressionType.ZLIB => new DeflateCompressorInputStream(stream, deflateParam)
      case CompressionType.GZIP => new GzipCompressorInputStream(stream)
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
