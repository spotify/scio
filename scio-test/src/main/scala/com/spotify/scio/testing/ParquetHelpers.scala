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

package com.spotify.scio.testing

import magnolify.parquet._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.parquet.io._

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.reflect.ClassTag

object ParquetHelpers {
  sealed trait ParquetFilterHelper[T] {
    def parquetFilter(filter: FilterPredicate): Iterable[T]
  }

  sealed trait ParquetProjectionHelper[T] {
    def parquetProject(schema: Schema): Iterable[T]
  }

  case class ParquetMagnolifyHelpers[T: ParquetType: ClassTag](records: Iterable[T])
      extends ParquetFilterHelper[T] {
    override def parquetFilter(filter: FilterPredicate): Iterable[T] = {
      val configuration = new Configuration()
      ParquetInputFormat.setFilterPredicate(configuration, filter)

      roundtrip(records, configuration)
    }

    private def roundtrip(records: Iterable[T], readConfiguration: Configuration): Iterable[T] = {
      val pt = implicitly[ParquetType[T]]
      val baos = new ByteArrayOutputStream()
      val writer = pt.writeBuilder(inMemoryOutputFile(baos)).build()

      records.foreach(writer.write)
      writer.close()

      val reader = pt
        .readBuilder(inMemoryInputFile(baos.toByteArray))
        .withConf(readConfiguration)
        .build()

      val roundtripped = Iterator.continually(reader.read()).takeWhile(_ != null).toSeq
      reader.close()
      roundtripped
    }
  }

  case class ParquetAvroHelpers[T <: GenericRecord: ClassTag](records: Iterable[T])
      extends ParquetFilterHelper[T]
      with ParquetProjectionHelper[T] {
    override def parquetProject(projection: Schema): Iterable[T] = {
      val configuration = new Configuration()
      AvroReadSupport.setRequestedProjection(configuration, projection)

      roundtrip(records, configuration)
    }

    override def parquetFilter(filter: FilterPredicate): Iterable[T] = {
      val configuration = new Configuration()
      ParquetInputFormat.setFilterPredicate(configuration, filter)

      roundtrip(records, configuration)
    }

    private def roundtrip(records: Iterable[T], readConfiguration: Configuration): Iterable[T] = {
      records.headOption match {
        case None =>
          records // empty iterable
        case Some(head) =>
          val schema = head.getSchema

          val baos = new ByteArrayOutputStream()
          val writer = AvroParquetWriter
            .builder[T](inMemoryOutputFile(baos))
            .withSchema(schema)
            .build()

          records.foreach(writer.write)
          writer.close()

          val reader = AvroParquetReader
            .builder[T](inMemoryInputFile(baos.toByteArray))
            .withConf(readConfiguration)
            .build()

          val roundtripped = Iterator.continually(reader.read()).takeWhile(_ != null).toSeq
          reader.close()
          roundtripped
      }
    }
  }

  // @Todo tensorflow helpers

  private def inMemoryOutputFile(baos: ByteArrayOutputStream): OutputFile = new OutputFile {
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

  private def inMemoryInputFile(bytes: Array[Byte]): InputFile = new InputFile {
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

  implicit def toParquetAvroHelpers[T <: GenericRecord: ClassTag](
    records: Iterable[T]
  ): ParquetAvroHelpers[T] = ParquetAvroHelpers(records)

  implicit def toParquetMagnolifyHelpers[T: ParquetType: ClassTag](
    records: Iterable[T]
  ): ParquetMagnolifyHelpers[T] = ParquetMagnolifyHelpers(records)
}
