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
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroParquetReader, AvroParquetWriter, AvroReadSupport}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.{ParquetInputFormat, ParquetReader}
import org.apache.parquet.io.InputFile

package object avro {
  implicit def toParquetAvroHelpers[T <: GenericRecord](
    records: Iterable[T]
  ): ParquetAvroHelpers[T] = new ParquetAvroHelpers(records)

  class ParquetAvroHelpers[U <: GenericRecord] private[testing] (
    records: Iterable[U]
  ) {
    def withProjection(projection: Schema): Iterable[U] = {
      val configuration = new Configuration()
      AvroReadSupport.setRequestedProjection(configuration, projection)

      roundtripAvro(
        records,
        inputFile => AvroParquetReader.builder[U](inputFile).withConf(configuration).build()
      )
    }

    def withProjection[V: ParquetType]: Iterable[V] = {
      val pt = implicitly[ParquetType[V]]

      roundtripAvro(records, inputFile => pt.readBuilder(inputFile).build())
    }

    def withFilter(filter: FilterPredicate): Iterable[U] = {
      val configuration = new Configuration()
      ParquetInputFormat.setFilterPredicate(configuration, filter)

      roundtripAvro(
        records,
        inputFile => AvroParquetReader.builder[U](inputFile).withConf(configuration).build()
      )
    }

    private def roundtripAvro[V](
      records: Iterable[U],
      readerFn: InputFile => ParquetReader[V]
    ): Iterable[V] = {
      records.headOption match {
        case None =>
          Iterable.empty[V] // empty iterable
        case Some(head) =>
          val schema = head.getSchema

          roundtrip(
            outputFile => AvroParquetWriter.builder[U](outputFile).withSchema(schema).build(),
            readerFn
          )(records)
      }
    }
  }
}
