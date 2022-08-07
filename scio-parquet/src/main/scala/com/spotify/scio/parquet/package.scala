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
    override def create(blockSizeHint: Long) = new ParquetOutputStream(Channels.newOutputStream(channel))
    override def createOrOverwrite(blockSizeHint: Long) = new ParquetOutputStream(Channels.newOutputStream(channel))
  }
}
