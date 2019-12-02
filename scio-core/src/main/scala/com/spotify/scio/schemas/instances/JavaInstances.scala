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
import com.spotify.scio.schemas.{ArrayType, MapType, RawRecord, Schema, Type}
import org.apache.beam.sdk.schemas.JavaBeanSchema
import org.apache.beam.sdk.schemas.Schema.FieldType

import scala.reflect.ClassTag
import org.apache.beam.sdk.schemas.Schema.LogicalType

trait JavaInstances {
  implicit val jByteSchema: Type[java.lang.Byte] =
    Type[java.lang.Byte](FieldType.BYTE)

  implicit val jBytesSchema: Type[Array[java.lang.Byte]] =
    Type[Array[java.lang.Byte]](FieldType.BYTES)

  implicit val jShortSchema: Type[java.lang.Short] =
    Type[java.lang.Short](FieldType.INT16)

  implicit val jIntegerSchema: Type[java.lang.Integer] =
    Type[java.lang.Integer](FieldType.INT32)

  implicit val jLongSchema: Type[java.lang.Long] =
    Type[java.lang.Long](FieldType.INT64)

  implicit val jFloatSchema: Type[java.lang.Float] =
    Type[java.lang.Float](FieldType.FLOAT)

  implicit val jDoubleSchema: Type[java.lang.Double] =
    Type[java.lang.Double](FieldType.DOUBLE)

  implicit val jBigDecimalSchema: Type[java.math.BigDecimal] =
    Type[java.math.BigDecimal](FieldType.DECIMAL)

  implicit val jBooleanSchema: Type[java.lang.Boolean] =
    Type[java.lang.Boolean](FieldType.BOOLEAN)

  implicit def jListSchema[T](implicit s: Schema[T]): Schema[java.util.List[T]] =
    ArrayType(s, identity, identity)

  implicit def jArrayListSchema[T](implicit s: Schema[T]): Schema[java.util.ArrayList[T]] =
    ArrayType(s, identity, l => new util.ArrayList[T](l))

  implicit def jMapSchema[K, V](
    implicit ks: Schema[K],
    vs: Schema[V]
  ): Schema[java.util.Map[K, V]] =
    MapType(ks, vs, identity, identity)

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: RawRecord[T] =
    RawRecord[T](new JavaBeanSchema())

  implicit def javaEnumSchema[T <: java.lang.Enum[T]: ClassTag]: Schema[T] =
    Type[T](FieldType.logicalType(new LogicalType[T, String] {
      private val clazz = scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]]
      private val className = clazz.getCanonicalName
      override def getIdentifier: String = className
      override def getBaseType: FieldType = FieldType.STRING
      override def toBaseType(input: T): String = input.name()
      override def toInputType(base: String): T =
        java.lang.Enum.valueOf[T](clazz, base)
      override def toString(): String =
        s"EnumLogicalType($className, String)"
    }))
}

private[schemas] object JavaInstances extends JavaInstances
