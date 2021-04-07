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
import java.util.function.BiConsumer

import scala.reflect.ClassTag
import scala.jdk.CollectionConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.Row
import BSchema.{Field => BField, FieldType => BFieldType}

object SchemaMaterializer {
  @inline private[scio] def fieldType[A](schema: Schema[A]): BFieldType =
    schema match {
      case Record(schemas, _, _) =>
        val out = new Array[BField](schemas.length)
        var i = 0
        while (i < schemas.length) {
          val (name, schema) = schemas(i)
          out.update(i, BField.of(name, fieldType(schema)))
          i = i + 1
        }
        BFieldType.row(BSchema.of(out: _*))
      case LogicalType(u)           => u
      case RawRecord(bschema, _, _) => BFieldType.row(bschema)
      case Type(t)                  => t
      case ArrayType(s, _, _)       => BFieldType.array(fieldType(s))
      case MapType(ks, vs, _, _)    => BFieldType.map(fieldType(ks), fieldType(vs))
      case OptionType(s)            => fieldType(s).withNullable(true)
    }

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchDecode[A](schema: Schema[A]): schema.Decode =
    schema match {
      case s @ Record(_, _, _)      => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case RawRecord(_, fromRow, _) => (fromRow.apply _).asInstanceOf[schema.Repr => A]
      case s @ Type(_)              => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ LogicalType(_)       => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ OptionType(_)        => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ ArrayType(_, _, _)   => (decode[s._F, s._T](s)(_)).asInstanceOf[schema.Repr => A]
      case s @ MapType(_, _, _, _) =>
        (decode[s._F, s._K, s._V](s)(_)).asInstanceOf[schema.Repr => A]
    }

  private def decode[A](record: Record[A])(v: record.Repr): A = {
    val size = v.getValues.size
    val vs = v.getValues
    val values = new Array[Any](size)
    var i = 0
    assert(size == record.schemas.length, s"Record $record and value $v sizes do not match")
    while (i < size) {
      val (_, schema) = record.schemas(i)
      val v = vs.get(i)
      values.update(i, dispatchDecode(schema)(v.asInstanceOf[schema.Repr]))
      i = i + 1
    }
    record.construct(values)
  }

  private[scio] def decode[A](schema: Type[A])(v: schema.Repr): A = v

  private def decode[A](schema: OptionType[A])(v: schema.Repr): Option[A] =
    Option(v).map(dispatchDecode(schema.schema))

  private def decode[A](schema: LogicalType[A])(v: schema.Repr): A =
    schema.fromBase(v)

  private def decode[F[_], A](schema: ArrayType[F, A])(v: schema.Repr): F[A] = {
    val values = new Array[Any](v.size)
    var i = 0
    while (i < v.size) {
      values.update(i, dispatchDecode[A](schema.schema)(v.get(i)))
      i = i + 1
    }
    schema.fromList(java.util.Arrays.asList(values.asInstanceOf[Array[A]]: _*))
  }

  private def decode[F[_, _], A, B](schema: MapType[F, A, B])(v: schema.Repr): F[A, B] = {
    val h = new util.HashMap[A, B]()

    v.forEach(new BiConsumer[schema.keySchema.Repr, schema.valueSchema.Repr] {
      override def accept(t: schema.keySchema.Repr, u: schema.valueSchema.Repr): Unit = {
        h.put(dispatchDecode[A](schema.keySchema)(t), dispatchDecode[B](schema.valueSchema)(u))
        ()
      }
    })

    schema.fromMap(h)
  }

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchEncode[A](schema: Schema[A], fieldType: BFieldType): schema.Encode =
    schema match {
      case s @ Record(_, _, _)    => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case RawRecord(_, _, toRow) => (toRow.apply _).asInstanceOf[A => schema.Repr]
      case s @ Type(_)            => (encode(s)(_)).asInstanceOf[A => schema.Repr]
      case s @ LogicalType(_)     => (encode(s)(_)).asInstanceOf[A => schema.Repr]
      case s @ OptionType(_)      => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ ArrayType(_, _, _) =>
        (encode[s._F, s._T](s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ MapType(_, _, _, _) =>
        (encode[s._F, s._K, s._V](s, fieldType)(_)).asInstanceOf[A => schema.Repr]
    }

  private def encode[A](schema: Record[A], fieldType: BFieldType)(v: A): schema.Repr = {
    val fields = schema.destruct(v)
    var i = 0
    val builder = Row.withSchema(fieldType.getRowSchema)
    while (i < fields.length) {
      val v = fields(i)
      val (_, s) = schema.schemas(i)
      val f = fieldType.getRowSchema.getFields.get(i)
      val value = dispatchEncode(s, f.getType)(v)
      builder.addValue(value)
      i = i + 1
    }
    builder.build()
  }

  private def encode[A](schema: LogicalType[A])(v: A): schema.Repr =
    schema.toBase(v)

  private def encode[A](schema: Type[A])(v: A): schema.Repr = v

  private def encode[A](schema: OptionType[A], fieldType: BFieldType)(v: Option[A]): schema.Repr =
    v.map(dispatchEncode(schema.schema, fieldType)(_))
      .getOrElse(null.asInstanceOf[schema.Repr])

  private def encode[F[_], A](schema: ArrayType[F, A], fieldType: BFieldType)(
    v: F[A]
  ): schema.Repr =
    schema
      .toList(v)
      .asScala
      .map(dispatchEncode(schema.schema, fieldType.getCollectionElementType): A => schema.schema.Repr)
      .asJava

  private def encode[F[_, _], A, B](schema: MapType[F, A, B], fieldType: BFieldType)(
    v: F[A, B]
  ): schema.Repr = {
    val h: util.Map[schema.keySchema.Repr, schema.valueSchema.Repr] = new util.HashMap()
    schema
      .toMap(v)
      .forEach(new BiConsumer[A, B] {
        override def accept(t: A, u: B): Unit = {
          val kd = dispatchEncode(schema.keySchema, fieldType.getCollectionElementType)(t)
          val vd = dispatchEncode(schema.valueSchema, fieldType.getCollectionElementType)(u)

          h.put(kd, vd)

          ()
        }
      })

    h
  }

  final def materialize[T](
    schema: Schema[T]
  ): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) =
    schema match {
      case RawRecord(bschema, fromRow, toRow) =>
        (bschema, toRow, fromRow)
      case record @ Record(_, _, _) =>
        val ft = fieldType(schema)
        val bschema = ft.getRowSchema

        def fromRow =
          new SerializableFunction[Row, T] {
            def apply(r: Row): T =
              decode[T](record)(r)
          }

        def toRow =
          new SerializableFunction[T, Row] {
            def apply(t: T): Row =
              encode[T](record, ft)(t)
          }

        (bschema, toRow, fromRow)
      case _ =>
        implicit val imp = schema
        val (bschema, to, from) = materialize(ScalarWrapper.schemaScalarWrapper[T])

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

  final def beamSchema[T](implicit schema: Schema[T]): BSchema = schema match {
    case s @ (_: Record[T] | _: RawRecord[T]) =>
      SchemaMaterializer.fieldType(s).getRowSchema
    case _ =>
      SchemaMaterializer.fieldType(Schema[ScalarWrapper[T]](ScalarWrapper.schemaScalarWrapper[T])).getRowSchema
  }
}
