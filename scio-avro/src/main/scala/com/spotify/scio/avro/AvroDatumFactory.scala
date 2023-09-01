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

package com.spotify.scio.avro

import org.apache.avro.Schema
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.reflect.{ReflectData, ReflectDatumReader, ReflectDatumWriter}
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils

/**
 * Custom AvroDatumFactory for avro AvroDatumFactory relying on avro reflect so that underlying
 * CharSequence type is String
 */
private[scio] class SpecificRecordDatumFactory[T](recordType: Class[T])
    extends AvroDatumFactory[T](recordType) {
  override def apply(writer: Schema, reader: Schema): DatumReader[T] = {
    val data = new ReflectData(recordType.getClassLoader)
    AvroUtils.addLogicalTypeConversions(data)
    new ReflectDatumReader[T](writer, reader, data)
  }

  override def apply(writer: Schema): DatumWriter[T] = {
    val data = new ReflectData(recordType.getClassLoader)
    AvroUtils.addLogicalTypeConversions(data)
    new ReflectDatumWriter[T](writer, data)
  }
}
