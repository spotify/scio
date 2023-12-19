/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.extra.json.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.json.{Decoder, Encoder, JsonIO}
import com.spotify.scio.extra.json.JsonIO.ReadParam
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment

/** Enhanced version of [[ScioContext]] with JSON methods. */
final class JsonScioContextOps(private val self: ScioContext) extends AnyVal {
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

trait ScioContextSyntax {
  implicit def jsonScioContextOps(sc: ScioContext): JsonScioContextOps =
    new JsonScioContextOps(sc)
}
