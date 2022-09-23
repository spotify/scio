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

import org.apache.parquet.io.{OutputFile, PositionOutputStream}

import java.io.OutputStream
import java.nio.channels.{Channels, WritableByteChannel}

package object parquet {
  class ParquetOutputStream(outputStream: OutputStream) extends PositionOutputStream {
    private var position = 0
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
}
