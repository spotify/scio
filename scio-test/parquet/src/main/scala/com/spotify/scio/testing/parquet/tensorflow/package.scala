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

import com.spotify.parquet.tensorflow.{
  TensorflowExampleParquetReader,
  TensorflowExampleParquetWriter,
  TensorflowExampleReadSupport
}
import com.spotify.scio.testing.parquet.ParquetTestUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat
import org.tensorflow.metadata.{v0 => tfmd}
import org.tensorflow.proto.example.Example

package object tensorflow {
  implicit def toParquetExampleHelpers(
    records: Iterable[Example]
  ): ParquetExampleHelpers = new ParquetExampleHelpers(records)

  class ParquetExampleHelpers private[testing] (records: Iterable[Example]) {
    def withProjection(schema: tfmd.Schema, projection: tfmd.Schema): Iterable[Example] = {
      val configuration = new Configuration()
      TensorflowExampleReadSupport.setExampleReadSchema(
        configuration,
        projection
      )
      TensorflowExampleReadSupport.setRequestedProjection(
        configuration,
        projection
      )

      roundtripExample(records, schema, configuration)
    }

    def withFilter(schema: tfmd.Schema, filter: FilterPredicate): Iterable[Example] = {
      val configuration = new Configuration()
      TensorflowExampleReadSupport.setExampleReadSchema(
        configuration,
        schema
      )
      ParquetInputFormat.setFilterPredicate(configuration, filter)

      roundtripExample(records, schema, configuration)
    }

    private def roundtripExample(
      records: Iterable[Example],
      schema: tfmd.Schema,
      readConfiguration: Configuration
    ): Iterable[Example] = roundtrip(
      outputFile => TensorflowExampleParquetWriter.builder(outputFile).withSchema(schema).build(),
      inputFile => {
        TensorflowExampleParquetReader.builder(inputFile).withConf(readConfiguration).build()
      }
    )(records)
  }
}
