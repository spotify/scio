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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}

import scala.reflect.ClassTag
import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.coders.{CoderRegistry, Coder => BCoder}
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.options.PipelineOptions
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
      case RawRecord(bschema, _, _) =>
        BFieldType.row(bschema)
      case Field(t)               => t
      case Fallback(_)            => BFieldType.BYTES
      case ArrayField(s, _, _)    => BFieldType.array(fieldType(s))
      case MapField(ks, vs, _, _) => BFieldType.map(fieldType(ks), fieldType(vs))
      case OptionField(s)         => fieldType(s).withNullable(true)
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
      case t @ Field(_) =>
        t
      case OptionField(s) =>
        OptionField(materializeSchema(reg, opt, s))
      case Fallback(c) =>
        Fallback[BCoder, A](CoderMaterializer.beam[A](reg, opt, c.asInstanceOf[Coder[A]]))
      case a @ ArrayField(s, t, f) =>
        ArrayField[a._F, a._T](materializeSchema(reg, opt, s), t, f)
      case m @ MapField(ks, vs, t, f) =>
        val mk = materializeSchema(reg, opt, ks)
        val mv = materializeSchema(reg, opt, vs)
        MapField[m._F, m._K, m._V](mk, mv, t, f)
    }

  import org.apache.beam.sdk.util.CoderUtils

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchDecode[A](schema: Schema[A]): schema.Decode =
    schema match {
      case s @ Record(_, _, _)      => (decode(s)(_)).asInstanceOf[schema.FieldType => A]
      case RawRecord(_, fromRow, _) => (fromRow.apply _).asInstanceOf[schema.FieldType => A]
      case s @ Field(_)             => (decode(s)(_)).asInstanceOf[schema.FieldType => A]
      case s @ OptionField(_)       => (decode(s)(_)).asInstanceOf[schema.FieldType => A]
      case s @ ArrayField(_, _, _)  => (decode[s._F, s._T](s)(_)).asInstanceOf[schema.FieldType => A]
      case s @ MapField(_, _, _, _) =>
        (decode[s._F, s._K, s._V](s)(_)).asInstanceOf[schema.FieldType => A]
      case s @ Fallback(_) =>
        (decode(s.asInstanceOf[Fallback[BCoder, A]])(_)).asInstanceOf[schema.FieldType => A]
    }

  private def decode[A](record: Record[A])(v: record.FieldType): A = {
    val size = v.getValues.size
    val vs = v.getValues
    val values = new Array[Any](size)
    var i = 0
    while (i < size) {
      val (_, schema) = record.schemas(i)
      val v = vs.get(i)
      values.update(i, dispatchDecode(schema)(v.asInstanceOf[schema.FieldType]))
      i = i + 1
    }
    record.construct(values)
  }
  private def decode[A](schema: Field[A])(v: schema.FieldType): A = v

  private def decode[A](schema: OptionField[A])(v: schema.FieldType): Option[A] =
    Option(dispatchDecode(schema.schema)(v))

  private def decode[F[_], A: ClassTag](schema: ArrayField[F, A])(v: schema.FieldType): F[A] = {
    val values = new Array[A](v.size)
    var i = 0
    while (i < v.size) {
      values.update(i, dispatchDecode[A](schema.schema)(v.get(i)))
      i = i + 1
    }
    schema.fromList(java.util.Arrays.asList(values: _*))
  }

  private def decode[F[_, _], A: ClassTag, B: ClassTag](schema: MapField[F, A, B])(
    v: schema.FieldType): F[A, B] = {
    val h = new util.HashMap[A, B]()

    v.forEach(new BiConsumer[schema.keySchema.FieldType, schema.valueSchema.FieldType] {
      override def accept(t: schema.keySchema.FieldType, u: schema.valueSchema.FieldType): Unit = {
        h.put(dispatchDecode[A](schema.keySchema)(t), dispatchDecode[B](schema.valueSchema)(u))
        ()
      }
    })

    schema.fromMap(h)
  }

  private def decode[A](schema: Fallback[BCoder, A])(v: schema.FieldType): A =
    CoderUtils.decodeFromByteArray(schema.coder, v)

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchEncode[A](schema: Schema[A], fieldType: BFieldType): schema.Encode =
    schema match {
      case s @ Record(_, _, _)    => (encode(s, fieldType)(_)).asInstanceOf[A => schema.FieldType]
      case RawRecord(_, _, toRow) => (toRow.apply _).asInstanceOf[A => schema.FieldType]
      case s @ Field(_)           => (encode(s)(_)).asInstanceOf[A => schema.FieldType]
      case s @ OptionField(_)     => (encode(s, fieldType)(_)).asInstanceOf[A => schema.FieldType]
      case s @ ArrayField(_, _, _) =>
        (encode[s._F, s._T](s, fieldType)(_)).asInstanceOf[A => schema.FieldType]
      case s @ MapField(_, _, _, _) =>
        (encode[s._F, s._K, s._V](s, fieldType)(_)).asInstanceOf[A => schema.FieldType]
      case s @ Fallback(_) =>
        (encode(s.asInstanceOf[Fallback[BCoder, A]])(_)).asInstanceOf[A => schema.FieldType]
    }

  private def encode[A](schema: Record[A], fieldType: BFieldType)(v: A): schema.FieldType = {
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

  private def encode[A](schema: Field[A])(v: A): schema.FieldType = v

  private def encode[A](schema: OptionField[A], fieldType: BFieldType)(
    v: Option[A]): schema.FieldType =
    v.map { dispatchEncode(schema.schema, fieldType)(_) }
      .getOrElse(null.asInstanceOf[schema.FieldType])

  private def encode[F[_], A](schema: ArrayField[F, A], fieldType: BFieldType)(
    v: F[A]): schema.FieldType = {
    schema
      .toList(v)
      .asScala
      .map(dispatchEncode(schema.schema, fieldType.getCollectionElementType))
      .asJava
  }

  private def encode[F[_, _], A, B](schema: MapField[F, A, B], fieldType: BFieldType)(
    v: F[A, B]): schema.FieldType = {
    val h: util.Map[schema.keySchema.FieldType, schema.valueSchema.FieldType] = new util.HashMap()
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

  private def encode[A](schema: Fallback[BCoder, A])(v: A): schema.FieldType =
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
      case Record(_, _, _) =>
        val ft = fieldType(schema)
        val bschema = ft.getRowSchema
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
      case _ =>
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
