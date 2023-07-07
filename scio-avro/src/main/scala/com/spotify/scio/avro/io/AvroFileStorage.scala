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

package com.spotify.scio.avro.io

import com.spotify.scio.io.FileStorage
import org.apache.avro.Schema
import org.apache.avro.file.{DataFileReader, SeekableInput}
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.specific.SpecificDatumReader
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.MatchResult.Metadata

import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object AvroFileStorage {
  @inline final def apply(path: String, suffix: String): AvroFileStorage =
    new AvroFileStorage(path, suffix)
}

final private[scio] class AvroFileStorage(path: String, suffix: String) {

  private def getAvroSeekableInput(meta: Metadata): SeekableInput =
    new SeekableInput {
      require(meta.isReadSeekEfficient)
      private val in = {
        val channel = FileSystems.open(meta.resourceId()).asInstanceOf[SeekableByteChannel]
        // metadata is lazy loaded on GCS FS and only triggered upon first read
        channel.read(ByteBuffer.allocate(1))
        // reset position
        channel.position(0)
      }

      override def read(b: Array[Byte], off: Int, len: Int): Int =
        in.read(ByteBuffer.wrap(b, off, len))

      override def tell(): Long = in.position()

      override def length(): Long = in.size()

      override def seek(p: Long): Unit = {
        in.position(p)
        ()
      }

      override def close(): Unit = in.close()
    }

  def avroFile[T](schema: Schema): Iterator[T] =
    avroFile(new GenericDatumReader[T](schema))

  def avroFile[T: ClassTag](): Iterator[T] =
    avroFile(new SpecificDatumReader[T](ScioUtil.classOf[T]))

  def avroFile[T](reader: GenericDatumReader[T]): Iterator[T] =
    FileStorage
      .listFiles(path, suffix)
      .map(m => DataFileReader.openReader(getAvroSeekableInput(m), reader))
      .map(_.iterator().asScala)
      .reduce(_ ++ _)

}
