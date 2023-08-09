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
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.DatumReader
import org.apache.avro.specific.{SpecificDatumReader, SpecificRecord}
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory

/**
 * AvroDatumFactory for [[GenericRecord]] forcing underlying [[CharSequence]] implementation to
 * [[String]]
 *
 * Avro default [[CharSequence]] implementation is [[org.apache.avro.util.Utf8]] which can't be used
 * when joining or as SMB keys as it doest not implement equals
 */
private[scio] object GenericRecordDatumFactory extends AvroDatumFactory.GenericDatumFactory {

  private class ScioGenericDatumReader extends GenericDatumReader[GenericRecord] {
    override def findStringClass(schema: Schema): Class[_] = super.findStringClass(schema) match {
      case cls if cls == classOf[CharSequence] => classOf[String]
      case cls                                 => cls
    }
  }
  override def apply(writer: Schema, reader: Schema): DatumReader[GenericRecord] = {
    val datumReader = new ScioGenericDatumReader()
    datumReader.setExpected(reader)
    datumReader.setSchema(writer)
    datumReader
  }
}

/**
 * AvroDatumFactory for [[SpecificRecord]] forcing underlying [[CharSequence]] implementation to
 * [[String]]
 *
 * Avro default [[CharSequence]] implementation is [[org.apache.avro.util.Utf8]] which can't be used
 * when joining or as SMB keys as it doest not implement equals
 */
private[scio] class SpecificRecordDatumFactory[T <: SpecificRecord](recordType: Class[T])
    extends AvroDatumFactory.SpecificDatumFactory[T](recordType) {
  private class ScioSpecificDatumReader extends SpecificDatumReader[T](recordType) {
    override def findStringClass(schema: Schema): Class[_] = super.findStringClass(schema) match {
      case cls if cls == classOf[CharSequence] => classOf[String]
      case cls                                 => cls
    }
  }
  override def apply(writer: Schema, reader: Schema): DatumReader[T] = {
    val datumReader = new ScioSpecificDatumReader()
    datumReader.setExpected(reader)
    datumReader.setSchema(writer)
    datumReader
  }
}
