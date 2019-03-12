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

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.coders.{CoderRegistry, Coder => BCoder}
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.options.PipelineOptions
import BSchema.{Field, FieldType}

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
      case RawRecord(bschema, _, _) =>
        FieldType.row(bschema)
      case Type(t)      => t
      case Fallback(_)  => FieldType.BYTES
      case Arr(s, _, _) => FieldType.array(fieldType(s))
      case Optional(s)  => fieldType(s).withNullable(true)
    }

  /**
   * Convert the Scio coders that may be embeded in this schema
   * to proper beam coders
   */
  private def materializeSchema[A](reg: CoderRegistry,
                                   opt: PipelineOptions,
                                   schema: Schema[A]): Schema[A] =
    schema match {
      case Record(schemas, construct, destruct) =>
        val schemasMat = schemas.map {
          case (n, s) => (n, materializeSchema(reg, opt, s))
        }
        Record[A](schemasMat, construct, destruct)
      case r @ RawRecord(_, _, _) =>
        r
      case t @ Type(_) =>
        t
      case Optional(s) =>
        Optional(materializeSchema(reg, opt, s))
      case Fallback(c) =>
        Fallback[BCoder, A](CoderMaterializer.beam[A](reg, opt, c.asInstanceOf[Coder[A]]))
      case a @ Arr(s, t, f) =>
        Arr[a._F, a._T](materializeSchema(reg, opt, s), t, f)
    }

  import org.apache.beam.sdk.util.CoderUtils

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchDecode[A](schema: Schema[A]): schema.Decode =
    schema match {
      case s @ Record(_, _, _)          => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ RawRecord(_, fromRow, _) => (fromRow.apply(_)).asInstanceOf[schema.Repr => A]
      case s @ Type(_)                  => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Optional(_)              => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Arr(_, _, _)             => (decode[s._F, s._T](s)(_)).asInstanceOf[schema.Repr => A]
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

  final def materialize[T](
    sc: ScioContext,
    schema: Schema[T]): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) =
    materialize(sc.pipeline.getCoderRegistry, sc.options, schema)

  final def materialize[T](
    reg: CoderRegistry,
    opt: PipelineOptions,
    schema: Schema[T]): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) = {
    schema match {
      case RawRecord(bschema, fromRow, toRow) =>
        (bschema, toRow, fromRow)
      case s @ Record(_, _, _) => {
        val ft = fieldType(schema)
        val bschema = ft.getRowSchema()
        val schemaMat = materializeSchema(reg, opt, schema).asInstanceOf[Record[T]]
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
        val (bschema, to, from) = materialize(reg, opt, implicitly[Schema[ScalarWrapper[T]]])
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
