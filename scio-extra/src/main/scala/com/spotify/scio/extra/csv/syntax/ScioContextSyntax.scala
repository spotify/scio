/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.extra.csv.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.csv.CsvIO
import com.spotify.scio.extra.csv.CsvIO.DefaultReadParams
import com.spotify.scio.values.SCollection
import kantan.csv.HeaderDecoder

trait ScioContextSyntax {
  implicit final class CsvScioContext(private val self: ScioContext) {
    @experimental
    def csvFile[T: HeaderDecoder: Coder](
      path: String,
      params: CsvIO.ReadParam = DefaultReadParams
    ): SCollection[T] =
      self.read(CsvIO.Read[T](path))(params)
  }
}
