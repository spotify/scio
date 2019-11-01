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
import scala.reflect.ClassTag

trait ScalaInstances {
  implicit val stringSchema: Type[String] =
    Type[String](FieldType.STRING)

  implicit val byteSchema: Type[Byte] =
    Type[Byte](FieldType.BYTE)

  implicit val bytesSchema: Type[Array[Byte]] =
    Type[Array[Byte]](FieldType.BYTES)

  implicit val sortSchema: Type[Short] =
    Type[Short](FieldType.INT16)

  implicit val intSchema: Type[Int] =
    Type[Int](FieldType.INT32)

  implicit val longSchema: Type[Long] =
    Type[Long](FieldType.INT64)

  implicit val floatSchema: Type[Float] =
    Type[Float](FieldType.FLOAT)

  implicit val doubleSchema: Type[Double] =
    Type[Double](FieldType.DOUBLE)

  implicit val bigDecimalSchema: Type[BigDecimal] =
    Type[BigDecimal](FieldType.DECIMAL)

  implicit val booleanSchema: Type[Boolean] =
    Type[Boolean](FieldType.BOOLEAN)

  implicit def optionSchema[T](implicit s: Schema[T]): Schema[Option[T]] =
    OptionType(s)

  implicit def arraySchema[T: ClassTag](implicit s: Schema[T]): Schema[Array[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toArray)

  implicit def listSchema[T](implicit s: Schema[T]): Schema[List[T]] =
    ArrayType(s, _.asJava, _.asScala.toList)

  implicit def mapSchema[K, V](implicit k: Schema[K], v: Schema[V]): Schema[Map[K, V]] =
    MapType(k, v, _.asJava, _.asScala.toMap)
}
