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

package com.spotify.scio.parquet.avro

import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.parquet.read.{ParquetRead, ReadSupportFactory}
import com.spotify.scio.util.ScioUtil
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.{SpecificData, SpecificRecord}
import org.apache.beam.sdk.io.FileIO.ReadableFile
import org.apache.beam.sdk.io.hadoop.SerializableConfiguration
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport}
import org.apache.parquet.filter2.predicate.FilterPredicate
import org.apache.parquet.hadoop.ParquetInputFormat

import scala.reflect.ClassTag

trait ParquetAvroRead { self: ParquetRead =>

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[GenericRecord]]s using the
   * supplied schema
   *
   * @param schema
   *   The Avro [[Schema]] to use for Parquet reads; can be a projection of the full file schema
   * @param projection
   *   an [[Schema]] used for Projection, made up of a subset of fields from the full schema
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readAvroGenericRecordFiles(
    schema: Schema,
    projection: Schema = null,
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[GenericRecord]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)

    configuration.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false)
    AvroReadSupport.setAvroReadSchema(configuration, schema)

    Option(projection).foreach(p => AvroReadSupport.setRequestedProjection(configuration, p))
    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))

    readFiles(ReadSupportFactory.avro, new SerializableConfiguration(configuration))
  }

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[SpecificRecord]]s.
   *
   * @param projection
   *   an [[Schema]] used for Projection, made up of a subset of fields from the full Avro type `T`
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readAvroFiles[T <: SpecificRecord: ClassTag](
    projection: Schema = null,
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)

    val recordClass = ScioUtil.classOf[T]
    val schema = SpecificData.get().getSchema(recordClass)
    AvroReadSupport.setAvroReadSchema(configuration, schema)

    Option(projection).foreach(p => AvroReadSupport.setRequestedProjection(configuration, p))
    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))

    readFiles(ReadSupportFactory.avro, new SerializableConfiguration(configuration))
  }
}

object ParquetAvroRead extends ParquetRead with ParquetAvroRead
