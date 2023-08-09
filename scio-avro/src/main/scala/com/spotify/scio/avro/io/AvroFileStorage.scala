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

import com.spotify.scio.avro.{GenericRecordDatumFactory, SpecificRecordDatumFactory}
import com.spotify.scio.io.FileStorage
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord, IndexedRecord}
import org.apache.avro.file.{DataFileReader, SeekableInput}
import org.apache.avro.specific.{SpecificData, SpecificDatumReader, SpecificRecord}
import com.spotify.scio.util.ScioUtil
import org.apache.avro.io.DatumReader
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory.SpecificDatumFactory
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

  def avroFile(schema: Schema): Iterator[GenericRecord] =
    avroFile(GenericRecordDatumFactory(schema, schema))

  def avroFile[T <: SpecificRecord: ClassTag](): Iterator[T] = {
    val recordClass = ScioUtil.classOf[T]
    val factory = new SpecificRecordDatumFactory[T](recordClass)
    val schema = SpecificData.get().getSchema(recordClass)
    avroFile(factory(schema, schema))
  }

  def avroFile[T <: IndexedRecord](reader: DatumReader[T]): Iterator[T] =
    FileStorage
      .listFiles(path, suffix)
      .map(m => DataFileReader.openReader(getAvroSeekableInput(m), reader))
      .map(_.iterator().asScala)
      .reduce(_ ++ _)
}
