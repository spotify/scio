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

import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase
import org.apache.beam.sdk.{io => beam}
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations
import org.apache.beam.sdk.io.{AvroIO, DynamicAvroDestinations, FileSystems}

import scala.concurrent.Future

/**
 * IO package for dynamic destinations. Import All.
 *
 * {{{
 * import com.spotify.scio.io.dynamic._
 * }}}
 */
package object dynamic {

  /**
   * Dynamic file destinations settings. Output path is `<path>/<destination>/part-<shard><suffix>`
   * for regular writes and `<path>/<destination>/part-<window>-<shard><suffix>` for windowed
   * writes, where `<destination>` is computed with `destinationFn`.
   * @param path path
   * @param default default `<destination>` for empty collections
   * @param windowedWrites preserves windowing of input elements and writes them to files based on
   *                       the element's window
   * @param numShards the number of shards to use, or 0 to let the system decide, must be > 0 when
   *                  `windowedWrites` is true
   */
  case class FileDestinations(path: String,
                              default: String = "default",
                              windowedWrites: Boolean = false,
                              numShards: Int = 0)

  /** Companion object for [[FileDestinations]]. */
  object FileDestinations {
    /** Create a windowed [[FileDestinations]]. */
    def windowed(path: String, numShards: Int, default: String = "default"): FileDestinations = {
      require(numShards > 0, "numShards must be > 0")
      FileDestinations(path, default, true, numShards)
    }
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic
   * destinations methods.
   */
  implicit class DynamicIoSCollection[T](val self: SCollection[T]) extends AnyVal {
    /**
     * Save this SCollection as Avro files specified by the
     * [[org.apache.beam.sdk.io.DynamicAvroDestinations DynamicAvroDestinations]].
     */
    def saveAsAvroFile(destinations: DynamicAvroDestinations[T, _, T], schema: Schema,
                       windowedWrites: Boolean, numShards: Int)
    : Future[Tap[T]] = {
      if (self.context.isTest) {
        throw new NotImplementedError(
          "Avro file with dynamic destinations cannot be used in a test context")
      } else {
        val tempDir = FileSystems.matchNewResource(self.context.options.getTempLocation, true)
        val cls = self.ct.runtimeClass.asInstanceOf[Class[T]]
        val t = if (classOf[SpecificRecordBase] isAssignableFrom cls) {
          AvroIO.write(cls)
        } else {
          AvroIO.writeGenericRecords(schema).asInstanceOf[AvroIO.Write[T]]
        }
        var transform = t.to(destinations).withTempDirectory(tempDir)
        if (windowedWrites) {
          transform = transform.withWindowedWrites().withNumShards(numShards)
        }
        self.applyInternal(transform)
      }

      Future.failed(
        new NotImplementedError("Avro file future with dynamic destinations not implemented"))
    }

    /**
     * Save this SCollection as Avro files specified by the destination function.
     */
    def saveAsAvroFile(fileDestination: FileDestinations,
                       schema: Schema = null,
                       suffix: String = ".avro")
                      (destinationFn: T => String): Future[Tap[T]] = {
      val destinations = DynamicDestinationsUtil.avroFn(
        fileDestination, suffix, destinationFn, schema)
      saveAsAvroFile(
        destinations, schema, fileDestination.windowedWrites, fileDestination.numShards)
    }

    /**
     * Save this SCollection as text files specified by the
     * [[org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations DynamicDestinations]].
     */
    def saveAsTextFile(destinations: DynamicDestinations[String, _, String],
                       windowedWrites: Boolean, numShards: Int)
    : Future[Tap[String]] = {
      val s = if (classOf[String] isAssignableFrom self.ct.runtimeClass) {
        self.asInstanceOf[SCollection[String]]
      } else {
        self.map(_.toString)
      }
      if (self.context.isTest) {
        throw new NotImplementedError(
          "Text file with dynamic destinations cannot be used in a test context")
      } else {
        val tempDir = FileSystems.matchNewResource(self.context.options.getTempLocation, true)
        var transform = beam.TextIO.write().to(destinations).withTempDirectory(tempDir)
        if (windowedWrites) {
          transform = transform.withWindowedWrites().withNumShards(numShards)
        }
        s.applyInternal(transform)
      }

      Future.failed(
        new NotImplementedError("Text file future with dynamic destinations not implemented"))
    }

    /**
     * Save this SCollection as text files specified by the destination function.
     */
    def saveAsTextFile(fileDestination: FileDestinations, suffix: String = ".txt")
                      (destinationFn: String => String): Future[Tap[String]] = {
      val destinations = DynamicDestinationsUtil.fileFn(fileDestination, suffix, destinationFn)
      saveAsTextFile(destinations, fileDestination.windowedWrites, fileDestination.numShards)
    }

  }

}
