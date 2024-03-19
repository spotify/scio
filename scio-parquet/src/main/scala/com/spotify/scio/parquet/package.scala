/*
 * Copyright 2022 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio

import org.apache.hadoop.conf.Configuration
import org.apache.parquet.io._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}
import java.nio.channels.{Channels, WritableByteChannel}

package object parquet {
  class ParquetOutputStream(outputStream: OutputStream) extends PositionOutputStream {
    private var position = 0L
    override def getPos: Long = position
    override def write(b: Int): Unit = {
      position += 1
      outputStream.write(b)
    }
    override def write(b: Array[Byte]): Unit = write(b, 0, b.length)
    override def write(b: Array[Byte], off: Int, len: Int): Unit = {
      outputStream.write(b, off, len)
      position += len
    }
    override def flush(): Unit = outputStream.flush()
    override def close(): Unit = outputStream.close()
  }

  class ParquetOutputFile(channel: WritableByteChannel) extends OutputFile {
    override def supportsBlockSize = false
    override def defaultBlockSize = 0
    override def create(blockSizeHint: Long) = new ParquetOutputStream(
      Channels.newOutputStream(channel)
    )
    override def createOrOverwrite(blockSizeHint: Long) = new ParquetOutputStream(
      Channels.newOutputStream(channel)
    )
  }

  object ParquetConfiguration {
    def empty(): Configuration =
      new Configuration()

    def of(entries: (String, Any)*): Configuration = {
      val conf = empty()
      entries.foreach { case (k, v) =>
        v match {
          case b: Boolean  => conf.setBoolean(k, b)
          case f: Float    => conf.setFloat(k, f)
          case d: Double   => conf.setDouble(k, d)
          case i: Int      => conf.setInt(k, i)
          case l: Long     => conf.setLong(k, l)
          case s: String   => conf.set(k, s)
          case c: Class[_] => conf.setClass(k, c, c)
          case _           => conf.set(k, v.toString)
        }
      }
      conf
    }

    private[parquet] def ofNullable(conf: Configuration): Configuration =
      Option(conf).getOrElse(empty())
  }

  private[parquet] def inMemoryOutputFile(baos: ByteArrayOutputStream): OutputFile =
    new BeamOutputFile(baos)

  private[parquet] def inMemoryInputFile(bytes: Array[Byte]): InputFile = new InputFile {
    override def getLength: Long = bytes.length

    override def newStream(): SeekableInputStream =
      new DelegatingSeekableInputStream(new ByteArrayInputStream(bytes)) {
        override def getPos: Long = bytes.length - getStream.available()
        override def mark(readlimit: Int): Unit = {
          if (readlimit != 0) {
            throw new UnsupportedOperationException(
              "In-memory seekable input stream is intended for testing only, can't mark past 0"
            )
          }
          super.mark(readlimit)
        }
        override def seek(newPos: Long): Unit = {
          getStream.reset()
          getStream.skip(newPos)
        }
      }
  }
}
