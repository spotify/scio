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
package com.spotify.scio.schemas.instances

import java.util

import com.spotify.scio.IsJavaBean
import com.spotify.scio.schemas.{ArrayField, Field, MapField, RawRecord, Schema}
import org.apache.beam.sdk.schemas.JavaBeanSchema
import org.apache.beam.sdk.schemas.Schema.FieldType

import scala.reflect.ClassTag

trait JavaInstances {

  implicit val jByteSchema: Field[java.lang.Byte] =
    Field[java.lang.Byte](FieldType.BYTE)

  implicit val jBytesSchema: Field[Array[java.lang.Byte]] =
    Field[Array[java.lang.Byte]](FieldType.BYTES)

  implicit val jShortSchema: Field[java.lang.Short] =
    Field[java.lang.Short](FieldType.INT16)

  implicit val jIntegerSchema: Field[java.lang.Integer] =
    Field[java.lang.Integer](FieldType.INT32)

  implicit val jLongSchema: Field[java.lang.Long] =
    Field[java.lang.Long](FieldType.INT64)

  implicit val jFloatSchema: Field[java.lang.Float] =
    Field[java.lang.Float](FieldType.FLOAT)

  implicit val jDoubleSchema: Field[java.lang.Double] =
    Field[java.lang.Double](FieldType.DOUBLE)

  implicit val jBigDecimalSchema: Field[java.math.BigDecimal] =
    Field[java.math.BigDecimal](FieldType.DECIMAL)

  implicit val jBooleanSchema: Field[java.lang.Boolean] =
    Field[java.lang.Boolean](FieldType.BOOLEAN)

  implicit def jListSchema[T](implicit s: Schema[T]): Schema[java.util.List[T]] =
    ArrayField(s, identity, identity)

  implicit def jArrayListSchema[T](implicit s: Schema[T]): Schema[java.util.ArrayList[T]] =
    ArrayField(s, identity, l => new util.ArrayList[T](l))

  implicit def jMapSchema[K, V](implicit ks: Schema[K],
                                vs: Schema[V]): Schema[java.util.Map[K, V]] =
    MapField(ks, vs, identity, identity)

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: RawRecord[T] =
    RawRecord[T](new JavaBeanSchema())

}
