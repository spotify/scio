/*
 * Copyright 2026 Spotify AB.
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

package com.spotify.scio.parquet.avro

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

import scala.jdk.CollectionConverters._

/**
 * parquet-avro reads strings into specific records as [[org.apache.avro.util.Utf8]] unless the read
 * schema tags them `avro.java.string: String`. Tagging them gives [[String]], like scio's avro
 * readers. Same as `AvroCompat.withJavaStringType` in scio-avro, which scio-parquet doesn't depend
 * on.
 */
private[scio] object AvroStringType {

  /**
   * Copy of `schema` with every string type, including map keys, tagged `avro.java.string: String`
   * unless already tagged.
   */
  def withJavaStringType(schema: Schema): Schema = {
    val copy = new Schema.Parser().parse(schema.toString)
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
    tag(copy, Set.empty)
    copy
  }
}
