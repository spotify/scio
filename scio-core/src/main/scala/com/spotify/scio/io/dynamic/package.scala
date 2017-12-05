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
import org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations
import org.apache.beam.sdk.io.{AvroIO, DynamicAvroDestinations, FileSystems, TextIO}

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
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with dynamic
   * destinations methods.
   */
  implicit class DynamicTextIoSCollection(val self: SCollection[String]) extends AnyVal {
    /**
     * Save this SCollection as text files specified by the
     * [[org.apache.beam.sdk.io.FileBasedSink.DynamicDestinations DynamicDestinations]].
     */
    def saveAsTextFile(destinations: DynamicDestinations[String, _, String])
    : Future[Tap[String]] = {
      if (self.context.isTest) {
        throw new NotImplementedError(
          "Text file with dynamic destinations cannot be used in a test context")
      } else {
        val tempDir = FileSystems.matchNewResource(self.context.options.getTempLocation, true)
        self.applyInternal(TextIO.write().to(destinations).withTempDirectory(tempDir))
      }

      Future.failed(
        new NotImplementedError("Text file future with dynamic destinations not implemented"))
    }

    /**
     * Save this SCollection as text files specified by the destination function.
     */
    def saveAsTextFile(path: String, suffix: String = ".txt", windowedWrites: Boolean = false,
                       defaultDestination: String = "default")
                      (destinationFn: String => String)
    : Future[Tap[String]] = {
      val destinations = DynamicDestinationsUtil.fileFn(
        path, suffix, windowedWrites, defaultDestination, destinationFn)
      saveAsTextFile(destinations)
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
    def saveAsAvroFile(destinations: DynamicAvroDestinations[T, _, T], schema: Schema)
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
        self.applyInternal(t.to(destinations).withTempDirectory(tempDir))
      }

      Future.failed(
        new NotImplementedError("Avro file future with dynamic destinations not implemented"))
    }

    /**
     * Save this SCollection as Avro files specified by the destination function.
     */
    def saveAsAvroFile(path: String, schema: Schema = null,
                       suffix: String = ".avro", windowedWrites: Boolean = false,
                       defaultDestination: String = "default")
                      (destinationFn: T => String)
    : Future[Tap[T]] = {
      val destinations = DynamicDestinationsUtil.avroFn(
        path, suffix, windowedWrites, defaultDestination, destinationFn, schema)
      saveAsAvroFile(destinations, schema)
    }
  }

}
