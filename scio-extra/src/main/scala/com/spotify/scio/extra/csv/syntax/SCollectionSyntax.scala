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

import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.csv.CsvIO
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.values.SCollection
import kantan.csv.{CsvConfiguration, HeaderDecoder, HeaderEncoder}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.transforms.ParDo

trait SCollectionSyntax {
  implicit final class WritableCsvSCollection[T](private val self: SCollection[T]) {
    @experimental
    def saveAsCsvFile(path: String, params: CsvIO.WriteParam = CsvIO.DefaultWriteParams)(
      implicit coder: Coder[T],
      enc: HeaderEncoder[T]
    ): ClosedTap[Nothing] = self.write(CsvIO.Write[T](path))(params)
  }

  implicit final class ReadableCsvFileSCollection(private val self: SCollection[ReadableFile]) {
    @experimental
    def readCsv[T: HeaderDecoder: Coder](csvConfiguration: CsvConfiguration): SCollection[T] =
      self
        .withName("Read CSV")
        .applyTransform(ParDo.of(CsvIO.ReadDoFn[T](csvConfiguration)))
  }
}
