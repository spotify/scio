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
package com.spotify.scio.extra.csv

import com.spotify.scio.ScioContext
import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.transforms.ParDo

/**
 * @see [[com.spotify.scio.extra.csv.CsvIO CsvIO]] for usage documentation
 */
trait CsvSyntax {

  import kantan.csv._

  implicit final class CsvScioContext(private val self: ScioContext) {
    @experimental
    def csvFile[T: HeaderDecoder: Coder](
      path: String,
      params: CsvIO.ReadParam = CsvIO.DEFAULT_READ_PARAMS
    ): SCollection[T] =
      self.read(CsvIO.Read[T](path))(params)
  }

  implicit final class CsvSCollection[T](private val self: SCollection[T]) {
    @experimental
    def saveAsCsvFile(path: String, params: CsvIO.WriteParam = CsvIO.DEFAULT_WRITE_PARAMS)(
      implicit headerEncoder: HeaderEncoder[T],
      coder: Coder[T]
    ): ClosedTap[Nothing] = self.write(CsvIO.Write[T](path))(params)
  }

  implicit final class ReadableCsvFileSCollection(private val self: SCollection[ReadableFile]) {
    @experimental
    def readCsv[T: HeaderDecoder: Coder](csvConfiguration: CsvConfiguration): SCollection[T] =
      self.applyTransform(ParDo.of(CsvIO.ReadDoFn[T](csvConfiguration)))
  }

}
