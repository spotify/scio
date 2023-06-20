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

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Scala case classes of type T
   *
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate, if desired
   * @param conf
   *   a Parquet [[Configuration]], if desired
   */
  def readTyped[T: ClassTag: ParquetType](
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = readTyped(
    identity[T],
    predicate,
    conf
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Scala case classes of type R, where
   * R is mapped from type T
   *
   * @param projectionFn
   *   a function mapping T => R
   */
  def readTyped[T: ClassTag: ParquetType, R](
    projectionFn: T => R
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = readTyped(
    projectionFn,
    null,
    null
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Scala case classes of type R, where
   * R is mapped from type T
   *
   * @param projectionFn
   *   a function mapping T => R
   * @param predicate
   *   a Parquet [[FilterApi]] predicate
   */
  def readTyped[T: ClassTag: ParquetType, R](
    projectionFn: T => R,
    predicate: FilterPredicate
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = readTyped(
    projectionFn,
    predicate,
    null
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Scala case classes of type R, where
   * R is mapped from type T
   *
   * @param projectionFn
   *   a function mapping T => R
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readTyped[T: ClassTag: ParquetType, R](
    projectionFn: T => R,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = readTyped(
    projectionFn,
    null,
    conf
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Scala case classes of type R, where
   * R is mapped from type T
   *
   * @param projectionFn
   *   a function mapping T => R
   * @param predicate
   *   a Parquet [[FilterApi]] predicate
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readTyped[T: ClassTag: ParquetType, R](
    projectionFn: T => R,
    predicate: FilterPredicate,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = {
    val configuration = ParquetConfiguration.ofNullable(conf)
    Option(predicate).foreach(p => ParquetInputFormat.setFilterPredicate(configuration, p))

    val cleanedFn = Functions.serializableFn(ClosureCleaner.clean(projectionFn))
    readFiles(ReadSupportFactory.typed[T], new SerializableConfiguration(configuration), cleanedFn)
  }

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[GenericRecord]]s using the
   * supplied schema
   *
   * @param schema
   *   The Avro [[Schema]] to use for Parquet reads; can be a projection of the full file schema
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate, if desired
   * @param conf
   *   a Parquet [[Configuration]], if desired
   */
  def readAvroGenericRecordFiles(
    schema: Schema,
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[GenericRecord]] =
    readAvroGenericRecordFiles(schema, identity[GenericRecord], predicate, conf)

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[GenericRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param schema
   *   The Avro [[Schema]] to use for Parquet reads; can be a projection of the full file schema
   * @param projectionFn
   *   a function mapping [[GenericRecord]] => T
   */
  def readAvroGenericRecordFiles[T](
    schema: Schema,
    projectionFn: GenericRecord => T
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = readAvroGenericRecordFiles(
    schema,
    projectionFn,
    null,
    null
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[GenericRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param schema
   *   The Avro [[Schema]] to use for Parquet reads; can be a projection of the full file schema
   * @param projectionFn
   *   a function mapping [[GenericRecord]] => T
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate
   */
  def readAvroGenericRecordFiles[T](
    schema: Schema,
    projectionFn: GenericRecord => T,
    predicate: FilterPredicate
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = readAvroGenericRecordFiles(
    schema,
    projectionFn,
    predicate,
    null
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[GenericRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param schema
   *   The Avro [[Schema]] to use for Parquet reads; can be a projection of the full file schema
   * @param projectionFn
   *   a function mapping [[GenericRecord]] => T
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readAvroGenericRecordFiles[T](
    schema: Schema,
    projectionFn: GenericRecord => T,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[T]] = readAvroGenericRecordFiles(
    schema,
    projectionFn,
    conf
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[GenericRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param schema
   *   The Avro [[Schema]] to use for Parquet reads; can be a projection of the full file schema
   * @param projectionFn
   *   a function mapping [[GenericRecord]] => T
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate
   * @param conf
   *   a Parquet [[Configuration]]
   */
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

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[SpecificRecord]]s
   *
   * @param projection
   *   an optional [[Schema]] used for Projection, made up of a subset of fields from the full Avro
   *   type `T`. If left unspecified, all fields from the generated class `T` will be read. Note
   *   that all fields excluded from the projection MUST be nullable in the Avro schema; if they are
   *   non-nullable, the resulting PTransform will fail serialization. You can solve this by adding
   *   a `projectionFn` param mapping type `T` to a serializable type `R` (i.e., a Scala case
   *   class): see [[readAvro(projection, projectionFn, predicate, conf)]]
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate, if desired
   * @param conf
   *   a Parquet [[Configuration]], if desired
   */
  def readAvro[T <: SpecificRecord: ClassTag](
    projection: Schema = null,
    predicate: FilterPredicate = null,
    conf: Configuration = null
  ): PTransform[PCollection[ReadableFile], PCollection[T]] =
    readAvro(projection, identity[T], predicate, conf)

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[SpecificRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param projection
   *   an [[Schema]] used for Projection, made up of a subset of fields from the full Avro type `T`
   * @param projectionFn
   *   a function mapping T => R
   */
  def readAvro[T <: SpecificRecord: ClassTag, R](
    projection: Schema,
    projectionFn: T => R
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = readAvro(
    projection,
    projectionFn,
    null,
    null
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[SpecificRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param projection
   *   an [[Schema]] used for Projection, made up of a subset of fields from the full Avro type `T`
   * @param projectionFn
   *   a function mapping T => R
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate
   */
  def readAvro[T <: SpecificRecord: ClassTag, R](
    projection: Schema,
    projectionFn: T => R,
    predicate: FilterPredicate
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = readAvro(
    projection,
    projectionFn,
    predicate,
    null
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[SpecificRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param projection
   *   an [[Schema]] used for Projection, made up of a subset of fields from the full Avro type `T`
   * @param projectionFn
   *   a function mapping T => R
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readAvro[T <: SpecificRecord: ClassTag, R](
    projection: Schema,
    projectionFn: T => R,
    conf: Configuration
  ): PTransform[PCollection[ReadableFile], PCollection[R]] = readAvro(
    projection,
    projectionFn,
    null,
    conf
  )

  /**
   * A ReadFiles implementation that reads Parquet file(s) into Avro [[SpecificRecord]]s using the
   * supplied schema, then applies a mapping function to convert the Avro records into type T
   *
   * @param projection
   *   an [[Schema]] used for Projection, made up of a subset of fields from the full Avro type `T`
   * @param projectionFn
   *   a function mapping T => R
   * @param predicate
   *   a Parquet [[FilterPredicate]] predicate
   * @param conf
   *   a Parquet [[Configuration]]
   */
  def readAvro[T <: SpecificRecord: ClassTag, R](
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
