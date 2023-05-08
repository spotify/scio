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

import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.util.{Functions, ScioUtil}
import com.twitter.chill.ClosureCleaner
import magnolify.parquet.ParquetType
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.reflect.ReflectData
import org.apache.avro.specific.SpecificRecordBase
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

object ParquetRead {

  def read[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    filePattern: String,
    projectionFn: SerializableFunction[T, R]
  ): PTransform[PBegin, PCollection[R]] =
    new PTransform[PBegin, PCollection[R]] {
      override def expand(input: PBegin): PCollection[R] = {
        input
          .apply(FileIO.`match`().filepattern(filePattern))
          .apply(FileIO.readMatches)
          .apply(readFiles(readSupportFactory, conf, projectionFn))
      }
    }

  def readFiles[T, R](
    readSupportFactory: ReadSupportFactory[T],
    conf: SerializableConfiguration,
    projectionFn: SerializableFunction[T, R]
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = {
    val sdf = new ParquetReadFn[T, R](readSupportFactory, conf, projectionFn)
    val tfx: PTransform[PCollection[_ <: ReadableFile], PCollection[R]] = ParDo.of(sdf)

    tfx.asInstanceOf[PTransform[PCollection[ReadableFile], PCollection[R]]]
  }

  def readTyped[T: ClassTag: Coder: ParquetType](
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = readTyped[T, T](
    identity,
    predicate,
    conf
  )

  def readTyped[T: ClassTag: Coder: ParquetType, R](
    projectionFn: T => R,
    predicate: FilterPredicate,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)
    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))

    val cleanedFn = Functions.serializableFn(ClosureCleaner.clean(projectionFn))
    readFiles(ReadSupportFactory.typed[T], new SerializableConfiguration(configuration), cleanedFn)
  }

  def readAvroGenericRecordFiles(
    schema: Schema,
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[GenericRecord]] =
    readAvroGenericRecordFiles[GenericRecord](schema, identity, predicate, conf)

  def readAvroGenericRecordFiles[T](
    schema: Schema,
    projectionFn: GenericRecord => T,
    predicate: FilterPredicate,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)
    configuration.setBoolean(AvroReadSupport.AVRO_COMPATIBILITY, false)
    AvroReadSupport.setAvroReadSchema(configuration, schema)
    AvroReadSupport.setRequestedProjection(configuration, schema)

    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))
    if (configuration.get(AvroReadSupport.AVRO_DATA_SUPPLIER) == null) {
      AvroReadSupport.setAvroDataSupplier(configuration, classOf[GenericDataSupplier])
    }

    val cleanedFn = Functions.serializableFn(ClosureCleaner.clean(projectionFn))
    readFiles(
      ReadSupportFactory.avro,
      new SerializableConfiguration(configuration),
      cleanedFn
    )
  }

  def readAvro[T <: SpecificRecordBase: ClassTag](
    projection: Schema = null,
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[T]] =
    readAvro[T, T](projection, identity, predicate, conf)

  def readAvro[T <: SpecificRecordBase: ClassTag, R](
    projection: Schema,
    projectionFn: T => R,
    predicate: FilterPredicate,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)

    val avroClass = ScioUtil.classOf[T]
    val readSchema = ReflectData.get().getSchema(avroClass)
    AvroReadSupport.setAvroReadSchema(configuration, readSchema)

    Option(projection).foreach(p => AvroReadSupport.setRequestedProjection(configuration, p))
    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))

    val cleanedFn = Functions.serializableFn(ClosureCleaner.clean(projectionFn))
    readFiles(
      ReadSupportFactory.avro[T],
      new SerializableConfiguration(configuration),
      cleanedFn
    )
  }
}
