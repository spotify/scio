/*
 * Copyright 2016 Spotify AB.
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

import com.spotify.scio.{IsJavaBean}
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import scala.language.higherKinds
import scala.reflect.{classTag, ClassTag}
import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils
import org.apache.beam.sdk.schemas.{
  Schema => BSchema,
  JavaBeanSchema,
  FieldValueGetter,
  FieldValueSetter
}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.values.Row
import BSchema.{Field, FieldType}

sealed trait Schema[T] {
  type Repr
  type Decode = Repr => T
  type Encode = T => Repr
}

final case class Record[T] private (schemas: Array[(String, Schema[Any])],
                                    construct: Seq[Any] => T,
                                    destruct: T => Array[Any])
    extends Schema[T] {
  type Repr = Row

}

object Record {
  def apply[T](implicit r: Record[T]): Record[T] = r
}

final case class Type[T](fieldType: FieldType) extends Schema[T] {
  type Repr = T
}
final case class Optional[T](schema: Schema[T]) extends Schema[Option[T]] {
  type Repr = schema.Repr
}
final case class Fallback[F[_], T](coder: F[T]) extends Schema[T] {
  type Repr = Array[Byte]
}

import java.util.{List => jList}
final case class Arr[F[_], T](schema: Schema[T],
                              toList: F[T] => jList[T],
                              fromList: jList[T] => F[T])
    extends Schema[F[T]] { // TODO: polymorphism ?
  type Repr = jList[schema.Repr]
  type _T = T
  type _F[A] = F[A]
}

private[scio] case class ScalarWrapper[T](value: T) extends AnyVal

trait LowPriorityFallbackSchema extends LowPrioritySchemaDerivation {
  def fallback[A: Coder]: Schema[A] =
    Fallback[Coder, A](Coder[A]) // ¯\_(ツ)_/¯
}

object Schema extends LowPriorityFallbackSchema {
  implicit val stringSchema = Type[String](FieldType.STRING)
  implicit val byteSchema = Type[Byte](FieldType.BYTE)
  implicit val bytesSchema = Type[Array[Byte]](FieldType.BYTES)
  implicit val sortSchema = Type[Short](FieldType.INT16)
  implicit val intSchema = Type[Int](FieldType.INT32)
  implicit val LongSchema = Type[Long](FieldType.INT64)
  implicit val floatSchema = Type[Float](FieldType.FLOAT)
  implicit val doubleSchema = Type[Double](FieldType.DOUBLE)
  implicit val bigdecimalSchema = Type[BigDecimal](FieldType.DECIMAL)
  implicit val booleanSchema = Type[Boolean](FieldType.BOOLEAN)
  // implicit def datetimeSchema = FSchema[](FieldType.DATETIME)

  implicit def optionSchema[T](implicit s: Schema[T]): Schema[Option[T]] = Optional(s)
  implicit def listSchema[T](implicit s: Schema[T]): Schema[List[T]] =
    Arr(s, _.asJava, _.asScala.toList)

  implicit def jCollectionSchema[T](implicit s: Schema[T]): Schema[java.util.List[T]] =
    Arr(s, identity, identity)

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: Schema[T] = {
    val rc = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    val schema =
      JavaBeanUtils.schemaFromJavaBeanClass(rc, JavaBeanSchema.GetterTypeSupplier.INSTANCE)
    val getters = JavaBeanUtils.getGetters(rc, schema, JavaBeanSchema.GetterTypeSupplier.INSTANCE)
    val setters = JavaBeanUtils.getSetters(rc, schema, new JavaBeanSchema.SetterTypeSupplier())

    val fields: Array[(String, Schema[Any])] =
      schema.getFields.asScala.map { f =>
        (f.getName, Type[Any](f.getType))
      }.toArray

    val construct: Seq[Any] => T = { ps =>
      val instance: T = rc.newInstance()
      ps.zip(setters.asScala).foreach {
        case (v, s) =>
          s.asInstanceOf[FieldValueSetter[T, Any]].set(instance, v)
      }
      instance
    }

    val destruct: T => Array[Any] = { t =>
      getters.asScala.map { g =>
        g.asInstanceOf[FieldValueGetter[T, Any]].get(t)
      }.toArray
    }
    Record(fields, construct, destruct)
  }

  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  // TODO: List and Map Schemas
  // TODO: Schema for traits ?
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
    val schemas = ps.toArray.map { p =>
      p.label -> p.typeclass.asInstanceOf[Schema[Any]]
    }
    Record(schemas, rawConstruct, destruct)
  }
}

trait LowPrioritySchemaDerivation {
  import language.experimental.macros, magnolia._

  type Typeclass[T] = Schema[T]

  def combine[T](ctx: CaseClass[Schema, T]): Record[T] = {
    val ps = ctx.parameters
    Derived.combineSchema(ps, ctx.rawConstruct)
  }

  implicit def gen[T]: Schema[T] = macro Magnolia.gen[T]
}

object SchemaMaterializer {

  import com.spotify.scio.ScioContext

  @inline private[scio] def fieldType[A](schema: Schema[A]): FieldType =
    schema match {
      case Record(schemas, _, _) =>
        val fields =
          schemas.map {
            case (name, schema) =>
              Field.of(name, fieldType(schema))
          }
        FieldType.row(BSchema.of(fields: _*))
      case Type(t)      => t
      case Fallback(_)  => FieldType.BYTES
      case Arr(s, _, _) => FieldType.array(fieldType(s))
      case Optional(s)  => fieldType(s).withNullable(true)
    }

  /**
   * Convert the Scio coders that may be embeded in this schema
   * to proper beam coders
   */
  private def materializeSchema[A](sc: ScioContext, schema: Schema[A]): Schema[A] =
    schema match {
      case Record(schemas, construct, destruct) =>
        val schemasMat = schemas.map {
          case (n, s) => (n, materializeSchema(sc, s))
        }
        Record[A](schemasMat, construct, destruct)
      case Type(t) =>
        Type(t)
      case Optional(s) =>
        Optional(materializeSchema(sc, s))
      case Fallback(c) =>
        Fallback[BCoder, A](CoderMaterializer.beam[A](sc, c.asInstanceOf[Coder[A]]))
      case a @ Arr(s, t, f) =>
        Arr[a._F, a._T](materializeSchema(sc, s), t, f)
    }

  import org.apache.beam.sdk.util.CoderUtils

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchDecode[A](schema: Schema[A]): schema.Decode =
    schema match {
      case s @ Record(_, _, _) => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Type(_)         => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Optional(_)     => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Arr(_, _, _)    => (decode[s._F, s._T](s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Fallback(_) =>
        (decode(s.asInstanceOf[Fallback[BCoder, A]])(_)).asInstanceOf[schema.Repr => A]
    }

  private def decode[A](schema: Record[A])(v: schema.Repr): A = {
    val values =
      v.getValues.asScala.zip(schema.schemas).map {
        case (v, (name, schema)) =>
          dispatchDecode(schema)(v.asInstanceOf[schema.Repr])
      }
    schema.construct(values)
  }
  private def decode[A](schema: Type[A])(v: schema.Repr): A = v
  private def decode[A](schema: Optional[A])(v: schema.Repr): Option[A] =
    Option(dispatchDecode(schema.schema)(v))
  private def decode[F[_], A: ClassTag](schema: Arr[F, A])(v: schema.Repr): F[A] =
    schema.fromList(v.asScala.map { v =>
      dispatchDecode[A](schema.schema)(v)
    }.asJava)
  private def decode[A](schema: Fallback[BCoder, A])(v: schema.Repr): A =
    CoderUtils.decodeFromByteArray(schema.coder, v)

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchEncode[A](schema: Schema[A], fieldType: FieldType): schema.Encode =
    schema match {
      case s @ Record(_, _, _) => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ Type(_)         => (encode(s)(_)).asInstanceOf[A => schema.Repr]
      case s @ Optional(_)     => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ Arr(_, _, _) =>
        (encode[s._F, s._T](s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ Fallback(_) =>
        (encode(s.asInstanceOf[Fallback[BCoder, A]])(_)).asInstanceOf[A => schema.Repr]
    }

  private def encode[A](schema: Record[A], fieldType: FieldType)(v: A): schema.Repr = {
    val builder = Row.withSchema(fieldType.getRowSchema())
    schema
      .destruct(v)
      .zip(schema.schemas)
      .zip(fieldType.getRowSchema.getFields.asScala)
      .map {
        case ((v, (_, s)), f) =>
          dispatchEncode(s, f.getType)(v)
      }
      .foreach(builder.addValue _)
    builder.build()
  }
  private def encode[A](schema: Type[A])(v: A): schema.Repr = v
  private def encode[A](schema: Optional[A], fieldType: FieldType)(v: Option[A]): schema.Repr =
    v.map { dispatchEncode(schema.schema, fieldType)(_) }.getOrElse(null.asInstanceOf[schema.Repr])
  private def encode[F[_], A](schema: Arr[F, A], fieldType: FieldType)(v: F[A]): schema.Repr = {
    // XXX: probably slow
    schema
      .toList(v)
      .asScala
      .map { x =>
        dispatchEncode(schema.schema, fieldType.getCollectionElementType)(x)
      }
      .asJava
  }
  private def encode[A](schema: Fallback[BCoder, A])(v: A): schema.Repr =
    CoderUtils.encodeToByteArray(schema.coder, v)

  def materialize[T](
    sc: ScioContext,
    schema: Schema[T]): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) = {

    schema match {
      case s @ Record(_, _, _) => {
        val ft = fieldType(schema)
        val bschema = ft.getRowSchema()
        val schemaMat = materializeSchema(sc, schema).asInstanceOf[Record[T]]
        def fromRow =
          new SerializableFunction[Row, T] {
            def apply(r: Row): T =
              decode[T](schemaMat)(r)
          }

        def toRow =
          new SerializableFunction[T, Row] {
            def apply(t: T): Row =
              encode[T](schemaMat, ft)(t)
          }

        (bschema, toRow, fromRow)
      }
      case s =>
        implicit val imp = schema
        val (bschema, to, from) = materialize(sc, implicitly[Schema[ScalarWrapper[T]]])
        def fromRow =
          new SerializableFunction[Row, T] {
            def apply(r: Row): T =
              from(r).value
          }

        def toRow =
          new SerializableFunction[T, Row] {
            def apply(t: T): Row =
              to(ScalarWrapper(t))
          }

        (bschema, toRow, fromRow)
    }
  }
}
