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

import org.apache.avro.{Conversion, Schema}
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{DatumReader, DatumWriter}
import org.apache.avro.specific.{
  SpecificData,
  SpecificDatumReader,
  SpecificDatumWriter,
  SpecificRecord
}
import org.apache.beam.sdk.extensions.avro.io.AvroDatumFactory

import scala.jdk.CollectionConverters._

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
  import SpecificRecordDatumFactory._
  private class ScioSpecificDatumReader extends SpecificDatumReader[T](recordType) {
    override def findStringClass(schema: Schema): Class[_] = super.findStringClass(schema) match {
      case cls if cls == classOf[CharSequence] => classOf[String]
      case cls                                 => cls
    }
  }

  override def apply(writer: Schema): DatumWriter[T] = {
    val datumWriter = new SpecificDatumWriter[T]()
    // avro 1.8 generated code does not add conversions to the data
    if (runtimeAvroVersion.exists(_.startsWith("1.8."))) {
      addLogicalTypeConversions(datumWriter.getData.asInstanceOf[SpecificData], writer)
    }
    datumWriter.setSchema(writer)
    datumWriter
  }

  override def apply(writer: Schema, reader: Schema): DatumReader[T] = {
    val datumReader = new ScioSpecificDatumReader()
    // avro 1.8 generated code does not add conversions to the data
    if (runtimeAvroVersion.exists(_.startsWith("1.8."))) {
      addLogicalTypeConversions(datumReader.getData.asInstanceOf[SpecificData], reader)
    }
    datumReader.getData
    datumReader.setExpected(reader)
    datumReader.setSchema(writer)
    datumReader
  }
}

private[scio] object SpecificRecordDatumFactory {

  @transient private lazy val runtimeAvroVersion: Option[String] =
    Option(classOf[Schema].getPackage.getImplementationVersion)

  private def addLogicalTypeConversions[T <: SpecificRecord](
    data: SpecificData,
    schema: Schema,
    seenSchemas: Set[Schema] = Set.empty
  ): Unit = {
    if (seenSchemas.contains(schema)) {
      return
    }

    schema.getType match {
      case Schema.Type.RECORD =>
        Option(data.getClass(schema)).foreach { clazz =>
          try {
            val field = clazz.getDeclaredField("conversions")
            field.setAccessible(true)
            val conversions = field.get(null).asInstanceOf[Array[Conversion[_]]]
            // remove null entries from the conversion array
            conversions.filter(_ != null).foreach(data.addLogicalTypeConversion)

            val updatedSeenSchemas = seenSchemas + schema
            schema.getFields.asScala.foreach { f =>
              addLogicalTypeConversions(data, f.schema(), updatedSeenSchemas)
            }
          } catch {
            case _: NoSuchFieldException | _: IllegalAccessException =>
          }
        }
      case Schema.Type.MAP =>
        addLogicalTypeConversions(data, schema.getValueType, seenSchemas)
      case Schema.Type.ARRAY =>
        addLogicalTypeConversions(data, schema.getElementType, seenSchemas)
      case Schema.Type.UNION =>
        schema.getTypes.asScala.foreach { t =>
          addLogicalTypeConversions(data, t, seenSchemas)
        }
      case _ =>
    }
  }

}
