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

import com.spotify.scio.schemas._
import org.apache.beam.sdk.schemas.Schema.FieldType

import scala.collection.JavaConverters._

trait ScalaInstances {

  implicit val stringSchema: Field[String] =
    Field[String](FieldType.STRING)

  implicit val byteSchema: Field[Byte] =
    Field[Byte](FieldType.BYTE)

  implicit val bytesSchema: Field[Array[Byte]] =
    Field[Array[Byte]](FieldType.BYTES)

  implicit val sortSchema: Field[Short] =
    Field[Short](FieldType.INT16)

  implicit val intSchema: Field[Int] =
    Field[Int](FieldType.INT32)

  implicit val longSchema: Field[Long] =
    Field[Long](FieldType.INT64)

  implicit val floatSchema: Field[Float] =
    Field[Float](FieldType.FLOAT)

  implicit val doubleSchema: Field[Double] =
    Field[Double](FieldType.DOUBLE)

  implicit val bigDecimalSchema: Field[BigDecimal] =
    Field[BigDecimal](FieldType.DECIMAL)

  implicit val booleanSchema: Field[Boolean] =
    Field[Boolean](FieldType.BOOLEAN)

  implicit def optionSchema[T](implicit s: Schema[T]): Schema[Option[T]] =
    OptionField(s)

  implicit def listSchema[T](implicit s: Schema[T]): Schema[List[T]] =
    ArrayField(s, _.asJava, _.asScala.toList)

  implicit def mapSchema[K, V](implicit k: Schema[K], v: Schema[V]): Schema[Map[K, V]] =
    MapField(k, v, _.asJava, _.asScala.toMap)

}
