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

import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.json.{Decoder, Encoder, JsonIO}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.util.FilenamePolicySupplier
import com.spotify.scio.values.SCollection
import io.circe.Printer
import org.apache.beam.sdk.io.Compression

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with JSON methods. */
final class JsonSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {
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
  )(implicit encoder: Encoder[T], decoder: Decoder[T], coder: Coder[T]): ClosedTap[T] =
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
trait SCollectionSyntax {
  implicit def jsonSCollectionOps[T](sc: SCollection[T]): JsonSCollectionOps[T] =
    new JsonSCollectionOps[T](sc)
}
