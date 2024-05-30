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

package com.spotify.scio.testing

import com.spotify.scio.testing.parquet.ParquetTestUtils._
import magnolify.parquet.ParquetType
import org.apache.avro.generic.GenericRecord
import org.tensorflow.proto.example.Example

package object parquet {

  object avro {
    implicit def toParquetAvroHelpers[T <: GenericRecord](
      records: Iterable[T]
    ): ParquetAvroHelpers[T] = ParquetAvroHelpers(records)
  }

  object types {
    implicit def toParquetMagnolifyHelpers[T: ParquetType](
      records: Iterable[T]
    ): ParquetMagnolifyHelpers[T] = ParquetMagnolifyHelpers(records)
  }

  object tensorflow {
    implicit def toParquetExampleHelpers(
      records: Iterable[Example]
    ): ParquetExampleHelpers = ParquetExampleHelpers(records)
  }
}
