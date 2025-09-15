/*
 * Copyright 2024 Spotify AB
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

package com.spotify.scio.extra.csv.dynamic.syntax

import com.spotify.scio.annotations.experimental
import com.spotify.scio.extra.csv.{CsvIO, CsvSink}
import com.spotify.scio.io.dynamic.syntax.DynamicSCollectionOps.writeDynamic
import com.spotify.scio.io.{ClosedTap, EmptyTap}
import com.spotify.scio.values.SCollection
import kantan.csv.{CsvConfiguration, HeaderEncoder}
import org.apache.beam.sdk.io.Compression

final class DynamicCsvSCollectionOps[T](
  private val self: SCollection[T]
) extends AnyVal {

  /** Save this SCollection of records as CSV files written to dynamic destinations. */
  @experimental
  def saveAsDynamicCsvFile(
    path: String,
    suffix: String = CsvIO.WriteParam.DefaultSuffix,
    prefix: String = CsvIO.WriteParam.DefaultPrefix,
    numShards: Int = CsvIO.WriteParam.DefaultNumShards,
    compression: Compression = CsvIO.WriteParam.DefaultCompression,
    tempDirectory: String = CsvIO.WriteParam.DefaultTempDirectory,
    csvConfig: CsvConfiguration = CsvIO.WriteParam.DefaultCsvConfig
  )(
    destinationFn: T => String
  )(implicit enc: HeaderEncoder[T]): ClosedTap[Nothing] = {
    if (self.context.isTest) {
      throw new NotImplementedError(
        "CSV file with dynamic destinations cannot be used in a test context"
      )
    } else {
      val sink = new CsvSink(csvConfig)
      val write = writeDynamic(
        path = path,
        destinationFn = destinationFn,
        numShards = numShards,
        prefix = prefix,
        suffix = suffix,
        tempDirectory = tempDirectory
      ).withCompression(compression).via(sink)
      self.applyInternal(write)
    }
    ClosedTap[Nothing](EmptyTap)
  }
}

trait SCollectionSyntax {
  implicit def dynamicCsvSCollectionOps[T](
    sc: SCollection[T]
  ): DynamicCsvSCollectionOps[T] =
    new DynamicCsvSCollectionOps(sc)
}
