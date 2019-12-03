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
import scala.collection.mutable
import scala.collection.SortedSet

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

  implicit def seqSchema[T](implicit s: Schema[T]): Schema[Seq[T]] =
    ArrayType(s, _.asJava, _.asScala.toList)

  implicit def traversableOnceSchema[T](implicit s: Schema[T]): Schema[TraversableOnce[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toList)

  implicit def iterableSchema[T](implicit s: Schema[T]): Schema[Iterable[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toList)

  implicit def arrayBufferSchema[T](implicit s: Schema[T]): Schema[mutable.ArrayBuffer[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.ArrayBuffer(xs.asScala: _*))

  implicit def bufferSchema[T](implicit s: Schema[T]): Schema[mutable.Buffer[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.Buffer(xs.asScala: _*))

  implicit def setSchema[T](implicit s: Schema[T]): Schema[Set[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toSet)

  implicit def mutableSetSchema[T](implicit s: Schema[T]): Schema[mutable.Set[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.Set(xs.asScala: _*))

  implicit def sortedSetSchema[T: Ordering](implicit s: Schema[T]): Schema[SortedSet[T]] =
    ArrayType(s, _.toList.asJava, xs => SortedSet(xs.asScala: _*))

  implicit def listBufferSchema[T](implicit s: Schema[T]): Schema[mutable.ListBuffer[T]] =
    ArrayType(s, _.toList.asJava, xs => mutable.ListBuffer.apply(xs.asScala: _*))

  implicit def vectorSchema[T](implicit s: Schema[T]): Schema[Vector[T]] =
    ArrayType(s, _.toList.asJava, _.asScala.toVector)

  implicit def mapSchema[K, V](implicit k: Schema[K], v: Schema[V]): Schema[Map[K, V]] =
    MapType(k, v, _.asJava, _.asScala.toMap)

  // TODO: WrappedArray ?

  implicit def mutableMapSchema[K, V](
    implicit k: Schema[K],
    v: Schema[V]
  ): Schema[mutable.Map[K, V]] =
    MapType(k, v, _.asJava, _.asScala)
}

private[schemas] object ScalaInstances extends ScalaInstances
