/*
 * Copyright 2026 Spotify AB
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
import org.apache.avro.generic.GenericData
import com.spotify.scio.util.AvroGeneratedTrust

import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap

import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Compatibility with avro runtime versions newer than the one scio compiles against. Avro APIs that
 * don't exist in all supported versions are accessed by reflection, and skipped when missing.
 */
object AvroCompat {

  val FastReaderProperty = "org.apache.avro.fastread"

  private lazy val setFastReaderEnabled: Option[Method] =
    Try(classOf[GenericData].getMethod("setFastReaderEnabled", classOf[Boolean])).toOption

  /**
   * Avro 1.10+ FastReaderBuilder (enabled by default since 1.12) doesn't call
   * `GenericDatumReader.findStringClass`, so [[org.apache.avro.util.Utf8]] leaks into
   * [[CharSequence]] fields despite scio's override. Used for generic records, where
   * [[withJavaStringType]] would change the records' schema. Setting
   * `org.apache.avro.fastread=true` explicitly keeps the fast reader.
   */
  private[scio] def disableFastReader(data: GenericData): Unit =
    if (!"true".equalsIgnoreCase(System.getProperty(FastReaderProperty))) {
      setFastReaderEnabled.foreach(_.invoke(data, java.lang.Boolean.FALSE))
    }

  // keyed by schema equality, values are reused so GenericDatumReader's resolver cache, keyed by
  // schema reference, keeps hitting
  private val javaStringSchemas = new ConcurrentHashMap[Schema, Schema]()

  /**
   * Copy of `schema` with every string type, including map keys, tagged `avro.java.string: String`
   * unless already tagged. Readers then decode strings as [[String]] instead of
   * [[org.apache.avro.util.Utf8]], also with the avro 1.10+ FastReaderBuilder which, unlike
   * `GenericDatumReader`, doesn't call `findStringClass` but reads the tag.
   *
   * Only for specific records, which keep the schema of their class. Generic records keep the
   * reader schema, and `GenericData.Record.equals` compares schemas, props included.
   */
  private[scio] def withJavaStringType(schema: Schema): Schema =
    javaStringSchemas.computeIfAbsent(
      schema,
      s => tagJavaString(new Schema.Parser().parse(s.toString))
    )

  private def tagJavaString(schema: Schema): Schema = {
    def tag(s: Schema, seen: Set[String]): Unit = s.getType match {
      case Schema.Type.STRING                                  => tagType(s)
      case Schema.Type.RECORD if !seen.contains(s.getFullName) =>
        s.getFields.asScala.foreach(f => tag(f.schema(), seen + s.getFullName))
      case Schema.Type.ARRAY => tag(s.getElementType, seen)
      case Schema.Type.MAP   =>
        tagType(s) // map keys
        tag(s.getValueType, seen)
      case Schema.Type.UNION => s.getTypes.asScala.foreach(tag(_, seen))
      case _                 =>
    }
    def tagType(s: Schema): Unit =
      if (s.getProp(GenericData.STRING_PROP) == null) {
        GenericData.setStringType(s, GenericData.StringType.String)
      }
    tag(schema, Set.empty)
    schema
  }

  /**
   * Trust avro generated classes on avro 1.12.1+, see
   * [[com.spotify.scio.util.AvroGeneratedTrust.install]].
   */
  def trustGeneratedClasses(): Unit = AvroGeneratedTrust.install()
}
