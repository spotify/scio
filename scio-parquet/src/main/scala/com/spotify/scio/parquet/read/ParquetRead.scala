/*
 * Copyright 2022 Spotify AB
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

package com.spotify.scio.parquet.read

import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.avro.ParquetAvroRead
import com.spotify.scio.util.{Functions, ScioUtil}
import com.twitter.chill.ClosureCleaner
import magnolify.parquet.ParquetType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.transforms.{PTransform, ParDo, SerializableFunction}
import org.apache.beam.sdk.values.{PBegin, PCollection}
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroReadSupport, GenericDataSupplier}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat

import scala.reflect.ClassTag

trait ParquetRead {

  def read[T](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    filePattern: String
  ): PTransform[PBegin, PCollection[T]] =
    new PTransform[PBegin, PCollection[T]] {
      override def expand(input: PBegin): PCollection[T] = {
        input
          .apply(FileIO.`match`().filepattern(filePattern))
          .apply(FileIO.readMatches)
          .apply(readFiles(readSupportFactory, conf))
      }
    }

  def readFiles[T](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = {
    val sdf = new ParquetReadFn[T](readSupportFactory, conf)
    val tfx: PTransform[PCollection[_ <: ReadableFile], PCollection[T]] = ParDo.of(sdf)

    tfx.asInstanceOf[PTransform[PCollection[ReadableFile], PCollection[T]]]
  }

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Scala case classes of type T
   *
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate, if desired
   * @param conf
   *   a Parquet [[Configuration]], if desired
   */
  def readTypedFiles[T: ParquetType](
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)
    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))

    readFiles(ReadSupportFactory.typed[T], new SerializableConfiguration(configuration))
  }
}

object ParquetRead extends ParquetRead with ParquetAvroRead
