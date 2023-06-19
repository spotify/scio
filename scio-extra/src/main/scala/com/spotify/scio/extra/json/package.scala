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

package com.spotify.scio.extra

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.json.JsonIO.ReadParam
import com.spotify.scio.util.FilenamePolicySupplier
import io.circe.Printer
import io.circe.generic.AutoDerivation
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment

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
  final case class DecodeError(error: io.circe.Error, input: String)

  /** Enhanced version of [[ScioContext]] with JSON methods. */
  implicit final class JsonScioContext(private val self: ScioContext) extends AnyVal {
    @experimental
    def jsonFile[T: Decoder: Coder](
      path: String,
      compression: Compression = JsonIO.ReadParam.DefaultCompression,
      emptyMatchTreatment: EmptyMatchTreatment = ReadParam.DefaultEmptyMatchTreatment,
      suffix: String = JsonIO.ReadParam.DefaultSuffix
    ): SCollection[T] = {
      implicit val encoder: Encoder[T] = new Encoder[T] {
        final override def apply(a: T): io.circe.Json = ???
      }
      self.read(JsonIO[T](path))(JsonIO.ReadParam(compression, emptyMatchTreatment, suffix))
    }
  }

  /** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JSON methods. */
  implicit final class JsonSCollection[T: Encoder: Decoder: Coder](private val self: SCollection[T])
      extends Serializable {
    @experimental
    def saveAsJsonFile(
      path: String,
      suffix: String = JsonIO.WriteParam.DefaultSuffix,
      numShards: Int = JsonIO.WriteParam.DefaultNumShards,
      compression: Compression = JsonIO.WriteParam.DefaultCompression,
      printer: Printer = JsonIO.WriteParam.DefaultPrinter,
      shardNameTemplate: String = JsonIO.WriteParam.DefaultShardNameTemplate,
      tempDirectory: String = JsonIO.WriteParam.DefaultTempDirectory,
      filenamePolicySupplier: FilenamePolicySupplier =
        JsonIO.WriteParam.DefaultFilenamePolicySupplier,
      prefix: String = JsonIO.WriteParam.DefaultPrefix
    ): ClosedTap[T] =
      self.write(JsonIO[T](path))(
        JsonIO.WriteParam(
          suffix,
          numShards,
          compression,
          printer,
          filenamePolicySupplier,
          prefix,
          shardNameTemplate,
          tempDirectory
        )
      )
  }
}
