/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.parquet.avro.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.parquet.avro.{ParquetGenericRecordIO, ParquetSpecificRecordIO}
import com.spotify.scio.parquet.avro.ParquetAvroIO.ReadParam
import com.spotify.scio.values.SCollection
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificRecord
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.filter2.predicate.FilterPredicate

import scala.reflect.ClassTag

/** Enhanced version of [[ScioContext]] with Parquet Avro methods. */
final class ScioContextOps(@transient private val self: ScioContext) extends AnyVal {

  def parquetAvroGenericRecordFile(
    path: String,
    schema: Schema,
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = ReadParam.DefaultSuffix,
    datumFactory: AvroDatumFactory[GenericRecord] = ReadParam.DefaultDatumFactory
  ): SCollection[GenericRecord] = {
    val param = ParquetGenericRecordIO.ReadParam(projection, predicate, conf, suffix, datumFactory)
    self.read(ParquetGenericRecordIO(path, schema))(param)
  }

  def parquetAvroFile[T <: SpecificRecord: ClassTag](
    path: String,
    projection: Schema = ReadParam.DefaultProjection,
    predicate: FilterPredicate = ReadParam.DefaultPredicate,
    conf: Configuration = ReadParam.DefaultConfiguration,
    suffix: String = ReadParam.DefaultSuffix,
    datumFactory: AvroDatumFactory[T] = ReadParam.DefaultDatumFactory
  ): SCollection[T] = {
    val param = ParquetSpecificRecordIO.ReadParam(projection, predicate, conf, suffix, datumFactory)
    self.read(ParquetSpecificRecordIO[T](path))(param)
  }
}

trait ScioContextSyntax {
  implicit def parquetAvroScioContextOps(c: ScioContext): ScioContextOps = new ScioContextOps(c)
}
