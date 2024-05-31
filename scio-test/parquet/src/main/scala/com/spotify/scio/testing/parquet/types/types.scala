/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.testing.parquet

import com.spotify.scio.testing.parquet.ParquetTestUtils._
import magnolify.parquet.ParquetType
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat

package object types {
  class ParquetMagnolifyHelpers[T: ParquetType] private[testing] (records: Iterable[T]) {
    def withFilter(filter: FilterPredicate): Iterable[T] = {
      val pt = implicitly[ParquetType[T]]

      val configuration = new Configuration()
      ParquetInputFormat.setFilterPredicate(configuration, filter)

      roundtrip(
        outputFile => pt.writeBuilder(outputFile).build(),
        inputFile => pt.readBuilder(inputFile).withConf(configuration).build()
      )(records)
    }
  }

  implicit def toParquetMagnolifyHelpers[T: ParquetType](
    records: Iterable[T]
  ): ParquetMagnolifyHelpers[T] = new ParquetMagnolifyHelpers(records)
}
