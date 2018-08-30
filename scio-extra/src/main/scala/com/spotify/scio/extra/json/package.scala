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

package com.spotify.scio.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import io.circe.Printer
import io.circe.generic.AutoDerivation
import org.apache.beam.sdk.io.Compression

import scala.concurrent.Future
import scala.reflect.ClassTag

/**
 * Main package for JSON APIs. Import all.
 *
 * This package uses [[https://circe.github.io/circe/ Circe]] for JSON handling under the hood.
 *
 * {{{
 * import com.spotify.scio.extra.json._
 *
 * // define a type-safe JSON schema
 * case class Record(i: Int, d: Double, s: String)
 *
 * // read JSON as case classes
 * sc.jsonFile[Record]("input.json")
 *
 * // write case classes as JSON
 * sc.parallelize((1 to 10).map(x => Record(x, x.toDouble, x.toString))
 *   .saveAsJsonFile("output")
 * }}}
 */
package object json extends AutoDerivation {

  type Encoder[T] = io.circe.Encoder[T]
  type Decoder[T] = io.circe.Decoder[T]

  /** A wrapper for `io.circe.Error` that also retains the original input string. */
  case class DecodeError(error: io.circe.Error, input: String)

  /** Enhanced version of [[ScioContext]] with JSON methods. */
  implicit class JsonScioContext(@transient val self: ScioContext) extends Serializable {
    def jsonFile[T: ClassTag : Encoder : Decoder](path: String): SCollection[T] =
      self.read(JsonIO[T](path))
  }

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JSON methods.
   */
  implicit class JsonSCollection[T : ClassTag : Encoder : Decoder]
  (@transient val self: SCollection[T]) extends Serializable {
    def saveAsJsonFile(path: String,
                       suffix: String = ".json",
                       numShards: Int = 0,
                       compression: Compression = Compression.UNCOMPRESSED,
                       printer: Printer = Printer.noSpaces): Future[Tap[T]] = {
      self.write(JsonIO[T](path))(JsonIO.WriteParam(suffix, numShards, compression, printer))
    }
  }

}
