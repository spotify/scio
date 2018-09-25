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

import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.file.CodecFactory
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.AvroIO.RecordFormatter
import org.apache.beam.sdk.io.{Compression, FileIO}
import org.apache.beam.sdk.{io => beam}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * IO package for dynamic destinations. Import All.
 *
 * {{{
 * import com.spotify.scio.io.dynamic._
 * }}}
 */
package object dynamic {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic
   * destinations methods.
   */
  implicit class DynamicIoSCollection[T](private val self: SCollection[T]) extends AnyVal {

    /**
     * Save this SCollection as Avro files specified by the destination function.
     */
    def saveAsDynamicAvroFile(path: String,
                              numShards: Int = 0,
                              schema: Schema = null,
                              suffix: String = ".avro",
                              codec: CodecFactory = CodecFactory.deflateCodec(6),
                              metadata: Map[String, AnyRef] = Map.empty)(
      destinationFn: T => String)(implicit ct: ClassTag[T]): Future[Tap[T]] = {
      if (self.context.isTest) {
        throw new NotImplementedError(
          "Avro file with dynamic destinations cannot be used in a test context")
      } else {
        val cls = ct.runtimeClass.asInstanceOf[Class[T]]
        val sink = {
          if (classOf[SpecificRecordBase] isAssignableFrom cls) {
            beam.AvroIO.sink(cls)
          } else {
            beam.AvroIO
              .sinkViaGenericRecords(schema, new RecordFormatter[GenericRecord] {
                override def formatRecord(element: GenericRecord, schema: Schema): GenericRecord =
                  element
              })
              .asInstanceOf[beam.AvroIO.Sink[T]]
          }
        }.withCodec(codec)
          .withMetadata(com.google.common.collect.Maps
            .newHashMap(metadata.asJava))
        val write =
          writeDynamic(path, numShards, suffix, destinationFn).via(sink)
        self.applyInternal(write)
      }

      Future.failed(
        new NotImplementedError("Avro file future with dynamic destinations not implemented"))
    }

    /**
     * Save this SCollection as text files specified by the destination function.
     */
    def saveAsDynamicTextFile(path: String,
                              numShards: Int = 0,
                              suffix: String = ".txt",
                              compression: Compression = Compression.UNCOMPRESSED)(
      destinationFn: String => String)(implicit ct: ClassTag[T]): Future[Tap[String]] = {
      val s = if (classOf[String] isAssignableFrom ct.runtimeClass) {
        self.asInstanceOf[SCollection[String]]
      } else {
        self.map(_.toString)
      }
      if (self.context.isTest) {
        throw new NotImplementedError(
          "Text file with dynamic destinations cannot be used in a test context")
      } else {
        val write = writeDynamic(path, numShards, suffix, destinationFn)
          .via(beam.TextIO.sink())
          .withCompression(compression)
        s.applyInternal(write)
      }

      Future.failed(
        new NotImplementedError("Text file future with dynamic destinations not implemented"))
    }

  }

  private def writeDynamic[A](path: String,
                              numShards: Int,
                              suffix: String,
                              destinationFn: A => String): FileIO.Write[String, A] = {
    FileIO
      .writeDynamic[String, A]()
      .to(path)
      .withNumShards(numShards)
      .by(Functions.serializableFn(destinationFn))
      .withDestinationCoder(StringUtf8Coder.of())
      .withNaming(Functions.serializableFn { destination: String =>
        FileIO.Write.defaultNaming(s"$destination/part", suffix)
      })
  }
}
