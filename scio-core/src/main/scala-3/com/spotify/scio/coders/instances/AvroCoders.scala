/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.coders.instances

import com.spotify.scio.coders.{AvroCoderMacros, Coder}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.specific.SpecificFixed
import org.apache.beam.sdk.coders.AvroCoder

import scala.reflect.ClassTag
import scala.quoted._

trait AvroCoders {

  /**
   * Create a Coder for Avro GenericRecord given the schema of the GenericRecord.
   *
   * @param schema
   *   AvroSchema for the Coder.
   * @return
   *   Coder[GenericRecord]
   */
  // TODO: Use a coder that does not serialize the schema
  def avroGenericRecordCoder(schema: Schema): Coder[GenericRecord] =
    Coder.beam(AvroCoder.of(schema))

  // XXX: similar to GenericAvroSerializer
  def avroGenericRecordCoder: Coder[GenericRecord] =
    Coder.beam(new SlowGenericRecordCoder)

  import org.apache.avro.specific.SpecificRecordBase
  inline given genAvro[T <: SpecificRecordBase]: Coder[T] =
    ${ AvroCoderMacros.staticInvokeCoder[T] }

  given avroSpecificFixedCoder[T <: SpecificFixed: ClassTag]: Coder[T] =
    SpecificFixedCoder[T]
}