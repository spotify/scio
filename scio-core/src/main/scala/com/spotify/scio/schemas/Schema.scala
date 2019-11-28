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

import java.util
import java.util.{List => jList, Map => jMap}

import com.spotify.scio.{FeatureFlag, IsJavaBean, MacroSettings}
import com.spotify.scio.schemas.instances.{
  AllInstances,
  AvroInstances,
  JavaInstances,
  JodaInstances,
  LowPriorityFallbackInstances,
  ScalaInstances
}
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.schemas.Schema.FieldType
import org.apache.beam.sdk.schemas.{JavaBeanSchema, SchemaProvider, Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{Row, TypeDescriptor}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.schemas.Schema.LogicalType

import scala.collection.{mutable, SortedSet}

object Schema extends JodaInstances with AvroInstances with LowPriorityFallbackInstances {
  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  final def logicalType[U, T: ClassTag](
    underlying: Type[U]
  )(toBase: T => U, fromBase: U => T): Schema[T] = {
    Type[T](FieldType.logicalType(new LogicalType[T, U] {
      private val clazz = scala.reflect.classTag[T].runtimeClass.asInstanceOf[Class[T]]
      private val className = clazz.getCanonicalName
      override def getIdentifier: String = className
      override def getBaseType: FieldType = underlying.fieldType
      override def toBaseType(input: T): U = toBase(input)
      override def toInputType(base: U): T = fromBase(base)
      override def toString(): String =
        s"LogicalType($className, ${underlying.fieldType.getTypeName()})"
    }))
  }

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

sealed trait Schema[T] {
  type Repr
  type Decode = Repr => T
  type Encode = T => Repr
}

final case class Record[T] private (
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

final case class Fallback[F[_], T](coder: F[T]) extends Schema[T] {
  type Repr = Array[Byte]
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

private object Derived extends Serializable {
  import magnolia._
  def combineSchema[T](ps: Seq[Param[Schema, T]], rawConstruct: Seq[Any] => T): Record[T] = {
    @inline def destruct(v: T): Array[Any] = {
      val arr = new Array[Any](ps.length)
      var i = 0
      while (i < ps.length) {
        val p = ps(i)
        arr.update(i, p.dereference(v))
        i = i + 1
      }
      arr
    }
    val schemas = ps.iterator.map { p =>
      p.label -> p.typeclass.asInstanceOf[Schema[Any]]
    }.toArray

    Record(schemas, rawConstruct, destruct)
  }
}

trait LowPrioritySchemaDerivation {
  import magnolia._

  import language.experimental.macros

  type Typeclass[T] = Schema[T]

  def combine[T](ctx: CaseClass[Schema, T]): Record[T] = {
    val ps = ctx.parameters
    Derived.combineSchema(ps, ctx.rawConstruct)
  }

  import com.spotify.scio.MagnoliaMacros
  implicit def gen[T]: Schema[T] = macro MagnoliaMacros.genWithoutAnnotations[T]
}

private[scio] trait SchemaMacroHelpers {
  import scala.reflect.macros._

  val ctx: blackbox.Context
  import ctx.universe._

  val cacheImplicitSchemas = MacroSettings.cacheImplicitSchemas(ctx)

  def untyped[A: ctx.WeakTypeTag](expr: ctx.Expr[Schema[A]]): ctx.Expr[Schema[A]] =
    ctx.Expr[Schema[A]](ctx.untypecheck(expr.tree.duplicate))

  def inferImplicitSchema[A: ctx.WeakTypeTag]: ctx.Expr[Schema[A]] =
    inferImplicitSchema(weakTypeOf[A]).asInstanceOf[ctx.Expr[Schema[A]]]

  def inferImplicitSchema(t: ctx.Type): ctx.Expr[Schema[_]] = {
    val tpe =
      cacheImplicitSchemas match {
        case FeatureFlag.Enable =>
          tq"_root_.shapeless.Cached[_root_.com.spotify.scio.schemas.Schema[$t]]"
        case _ =>
          tq"_root_.com.spotify.scio.schemas.Schema[$t]"
      }

    val tp = ctx.typecheck(tpe, ctx.TYPEmode).tpe
    val typedTree = ctx.inferImplicitValue(tp, silent = false)
    val untypedTree = ctx.untypecheck(typedTree.duplicate)

    cacheImplicitSchemas match {
      case FeatureFlag.Enable =>
        ctx.Expr[Schema[_]](q"$untypedTree.value")
      case _ =>
        ctx.Expr[Schema[_]](untypedTree)
    }
  }

  implicit def liftTupleTag[A: ctx.WeakTypeTag]: Liftable[TupleTag[A]] = Liftable[TupleTag[A]] {
    x =>
      q"new _root_.org.apache.beam.sdk.values.TupleTag[${weakTypeOf[A]}](${x.getId()})"
  }
}
