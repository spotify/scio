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

package com.spotify.scio.schemas

import java.util.{List => jList, Map => jMap}

import com.spotify.scio.schemas.instances.{
  AvroInstances,
  JavaInstances,
  JodaInstances,
  LowPrioritySchemaDerivation,
  ScalaInstances
}
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.schemas.Schema.FieldType
import org.apache.beam.sdk.schemas.{SchemaProvider, Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{Row, TypeDescriptor}
import com.twitter.chill.ClosureCleaner

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import scala.collection.{mutable, SortedSet}
import com.spotify.scio.IsJavaBean

object Schema extends JodaInstances with AvroInstances with LowPrioritySchemaDerivation {
  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  implicit val jByteSchema: Type[java.lang.Byte] = JavaInstances.jByteSchema
  implicit val jBytesSchema: Type[Array[java.lang.Byte]] = JavaInstances.jBytesSchema
  implicit val jShortSchema: Type[java.lang.Short] = JavaInstances.jShortSchema
  implicit val jIntegerSchema: Type[java.lang.Integer] = JavaInstances.jIntegerSchema
  implicit val jLongSchema: Type[java.lang.Long] = JavaInstances.jLongSchema
  implicit val jFloatSchema: Type[java.lang.Float] = JavaInstances.jFloatSchema
  implicit val jDoubleSchema: Type[java.lang.Double] = JavaInstances.jDoubleSchema
  implicit val jBigDecimalSchema: Type[java.math.BigDecimal] = JavaInstances.jBigDecimalSchema
  implicit val jBooleanSchema: Type[java.lang.Boolean] = JavaInstances.jBooleanSchema
  implicit def jListSchema[T: Schema]: Schema[java.util.List[T]] =
    JavaInstances.jListSchema
  implicit def jArrayListSchema[T: Schema]: Schema[java.util.ArrayList[T]] =
    JavaInstances.jArrayListSchema
  implicit def jMapSchema[K: Schema, V: Schema]: Schema[java.util.Map[K, V]] =
    JavaInstances.jMapSchema
  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: RawRecord[T] =
    JavaInstances.javaBeanSchema
  implicit def javaEnumSchema[T <: java.lang.Enum[T]: ClassTag]: Schema[T] =
    JavaInstances.javaEnumSchema
  implicit def jLocalDate: Type[java.time.LocalDate] = JavaInstances.jLocalDate

  implicit val stringSchema: Type[String] = ScalaInstances.stringSchema
  implicit val byteSchema: Type[Byte] = ScalaInstances.byteSchema
  implicit val bytesSchema: Type[Array[Byte]] = ScalaInstances.bytesSchema
  implicit val sortSchema: Type[Short] = ScalaInstances.sortSchema
  implicit val intSchema: Type[Int] = ScalaInstances.intSchema
  implicit val longSchema: Type[Long] = ScalaInstances.longSchema
  implicit val floatSchema: Type[Float] = ScalaInstances.floatSchema
  implicit val doubleSchema: Type[Double] = ScalaInstances.doubleSchema
  implicit val bigDecimalSchema: Type[BigDecimal] = ScalaInstances.bigDecimalSchema
  implicit val booleanSchema: Type[Boolean] = ScalaInstances.booleanSchema
  implicit def optionSchema[T: Schema]: Schema[Option[T]] =
    ScalaInstances.optionSchema
  implicit def arraySchema[T: Schema: ClassTag]: Schema[Array[T]] =
    ScalaInstances.arraySchema
  implicit def listSchema[T: Schema]: Schema[List[T]] =
    ScalaInstances.listSchema
  implicit def seqSchema[T: Schema]: Schema[Seq[T]] =
    ScalaInstances.seqSchema
  implicit def traversableOnceSchema[T: Schema]: Schema[TraversableOnce[T]] =
    ScalaInstances.traversableOnceSchema
  implicit def iterableSchema[T: Schema]: Schema[Iterable[T]] =
    ScalaInstances.iterableSchema
  implicit def arrayBufferSchema[T: Schema]: Schema[mutable.ArrayBuffer[T]] =
    ScalaInstances.arrayBufferSchema
  implicit def bufferSchema[T: Schema]: Schema[mutable.Buffer[T]] =
    ScalaInstances.bufferSchema
  implicit def setSchema[T: Schema]: Schema[Set[T]] =
    ScalaInstances.setSchema
  implicit def mutableSetSchema[T: Schema]: Schema[mutable.Set[T]] =
    ScalaInstances.mutableSetSchema
  implicit def sortedSetSchema[T: Schema: Ordering]: Schema[SortedSet[T]] =
    ScalaInstances.sortedSetSchema
  implicit def listBufferSchema[T: Schema]: Schema[mutable.ListBuffer[T]] =
    ScalaInstances.listBufferSchema
  implicit def vectorSchema[T: Schema]: Schema[Vector[T]] =
    ScalaInstances.vectorSchema
  implicit def mapSchema[K: Schema, V: Schema]: Schema[Map[K, V]] =
    ScalaInstances.mapSchema
  implicit def mutableMapSchema[K: Schema, V: Schema]: Schema[mutable.Map[K, V]] =
    ScalaInstances.mutableMapSchema
}

sealed trait Schema[T] extends Serializable {
  type Repr
  type Decode = Repr => T
  type Encode = T => Repr
}

// Scio specific implementation of LogicalTypes
// Workarounds https://issues.apache.org/jira/browse/BEAM-8888
// Scio's Logical types are materialized to beam built-in types

sealed trait LogicalType[T] extends Schema[T] {
  def underlying: BSchema.FieldType
  def toBase(v: T): Repr
  def fromBase(base: Repr): T
  override def toString(): String = s"LogicalType($underlying)"
}

object LogicalType {
  def apply[T, U](u: BSchema.FieldType, t: T => U, f: U => T): LogicalType[T] = {
    require(
      u.getTypeName() != BSchema.TypeName.LOGICAL_TYPE,
      "Beam's logical types are not supported"
    )
    val ct = ClosureCleaner.clean(t)
    val cf = ClosureCleaner.clean(f)
    new LogicalType[T] {
      type Repr = U
      val underlying = u
      def toBase(v: T): U = ct(v)
      def fromBase(u: U): T = cf(u)
    }
  }

  def unapply[T](logicalType: LogicalType[T]): Option[BSchema.FieldType] =
    Some(logicalType.underlying)
}

final case class Record[T] private[schemas] (
  schemas: Array[(String, Schema[Any])],
  construct: Seq[Any] => T,
  destruct: T => Array[Any]
) extends Schema[T] {
  type Repr = Row

  override def toString: String =
    s"Record(${schemas.toList}, $construct, $destruct)"
}

object Record {
  @inline final def apply[T](implicit r: Record[T]): Record[T] = r
}

final case class RawRecord[T](
  schema: BSchema,
  fromRow: SerializableFunction[Row, T],
  toRow: SerializableFunction[T, Row]
) extends Schema[T] {
  type Repr = Row
}

object RawRecord {
  final def apply[T: ClassTag](provider: SchemaProvider): RawRecord[T] = {
    val td = TypeDescriptor.of(ScioUtil.classOf[T])
    val schema = provider.schemaFor(td)
    def toRow = provider.toRowFunction(td)
    def fromRow = provider.fromRowFunction(td)
    RawRecord(schema, fromRow, toRow)
  }
}

final case class Type[T](fieldType: FieldType) extends Schema[T] {
  type Repr = T
}

final case class OptionType[T](schema: Schema[T]) extends Schema[Option[T]] {
  type Repr = schema.Repr
}

final case class ArrayType[F[_], T](
  schema: Schema[T],
  toList: F[T] => jList[T],
  fromList: jList[T] => F[T]
) extends Schema[F[T]] { // TODO: polymorphism ?
  type Repr = jList[schema.Repr]
  type _T = T
  type _F[A] = F[A]
}

final case class MapType[F[_, _], K, V](
  keySchema: Schema[K],
  valueSchema: Schema[V],
  toMap: F[K, V] => jMap[K, V],
  fromMap: jMap[K, V] => F[K, V]
) extends Schema[F[K, V]] {
  type Repr = jMap[keySchema.Repr, valueSchema.Repr]

  type _K = K
  type _V = V
  type _F[XK, XV] = F[XK, XV]
}

private[scio] case class ScalarWrapper[T](value: T) extends AnyVal
object ScalarWrapper {
  implicit def schemaScalarWrapper[T: Schema]: Schema[ScalarWrapper[T]] =
    Record(
      Array("value" -> Schema[T].asInstanceOf[Schema[Any]]),
      vs => ScalarWrapper(vs.head.asInstanceOf[T]),
      w => Array(w.value))
}

private[scio] object SchemaTypes {
  private[this] def compareRows(s1: BSchema.FieldType, s2: BSchema.FieldType): Boolean = {
    val s1Types = s1.getRowSchema.getFields.asScala.map(_.getType)
    val s2Types = s2.getRowSchema.getFields.asScala.map(_.getType)
    (s1Types.length == s2Types.length) &&
    s1Types.zip(s2Types).forall { case (l, r) => equal(l, r) }
  }

  def equal(s1: BSchema.FieldType, s2: BSchema.FieldType): Boolean =
    (s1.getTypeName == s2.getTypeName) && (s1.getTypeName match {
      case BSchema.TypeName.ROW =>
        compareRows(s1, s2)
      case BSchema.TypeName.ARRAY =>
        equal(s1.getCollectionElementType, s2.getCollectionElementType)
      case BSchema.TypeName.MAP =>
        equal(s1.getMapKeyType, s2.getMapKeyType) && equal(s1.getMapValueType, s2.getMapValueType)
      case _ if s1.getNullable == s2.getNullable => true
      case _                                     => false
    })
}
