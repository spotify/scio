/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.testing.parquet

import org.apache.parquet.hadoop.{ParquetReader, ParquetWriter}
import org.apache.parquet.io._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

private[parquet] object ParquetTestUtils {

  def roundtrip[T, U](
    writerFn: OutputFile => ParquetWriter[T],
    readerFn: InputFile => ParquetReader[U]
  )(
    records: Iterable[T]
  ): Iterable[U] = {
    val baos = new ByteArrayOutputStream()
    val writer = writerFn(new InMemoryOutputFile(baos))

    records.foreach(writer.write)
    writer.close()

    val reader = readerFn(new InMemoryInputFile(baos.toByteArray))
    val roundtripped = Iterator.continually(reader.read()).takeWhile(_ != null).toSeq
    reader.close()
    roundtripped
  }

  private class InMemoryOutputFile(baos: ByteArrayOutputStream) extends OutputFile {
    override def create(blockSizeHint: Long): PositionOutputStream = newPositionOutputStream()

    override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream =
      newPositionOutputStream()

    override def supportsBlockSize(): Boolean = false

    override def defaultBlockSize(): Long = 0L

    private def newPositionOutputStream(): PositionOutputStream = new PositionOutputStream {
      var pos: Long = 0

      override def getPos: Long = pos

      override def write(b: Int): Unit = {
        pos += 1
        baos.write(b)
      }

      override def write(b: Array[Byte], off: Int, len: Int): Unit = {
        baos.write(b, off, len)
        pos += len
      }

      override def write(b: Array[Byte]): Unit = write(b, 0, b.length)

      override def flush(): Unit = baos.flush()

      override def close(): Unit = baos.close()
    }
  }

  private class InMemoryInputFile(bytes: Array[Byte]) extends InputFile {
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
