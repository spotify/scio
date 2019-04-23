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
import org.apache.beam.sdk.options.PipelineOptionsFactory
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
      case Type(t)               => t
      case Fallback(_)           => BFieldType.BYTES
      case ArrayType(s, _, _)    => BFieldType.array(fieldType(s))
      case MapType(ks, vs, _, _) => BFieldType.map(fieldType(ks), fieldType(vs))
      case OptionType(s)         => fieldType(s).withNullable(true)
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
      case OptionType(s) =>
        OptionType(materializeSchema(reg, opt, s))
      case Fallback(c) =>
        Fallback[BCoder, A](CoderMaterializer.beam[A](reg, opt, c.asInstanceOf[Coder[A]]))
      case a @ ArrayType(s, t, f) =>
        ArrayType[a._F, a._T](materializeSchema(reg, opt, s), t, f)
      case m @ MapType(ks, vs, t, f) =>
        val mk = materializeSchema(reg, opt, ks)
        val mv = materializeSchema(reg, opt, vs)
        MapType[m._F, m._K, m._V](mk, mv, t, f)
    }

  import org.apache.beam.sdk.util.CoderUtils

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchDecode[A](schema: Schema[A]): schema.Decode =
    schema match {
      case s @ Record(_, _, _)      => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case RawRecord(_, fromRow, _) => (fromRow.apply _).asInstanceOf[schema.Repr => A]
      case s @ Type(_)              => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ OptionType(_)        => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ ArrayType(_, _, _)   => (decode[s._F, s._T](s)(_)).asInstanceOf[schema.Repr => A]
      case s @ MapType(_, _, _, _) =>
        (decode[s._F, s._K, s._V](s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Fallback(_) =>
        (decode(s.asInstanceOf[Fallback[BCoder, A]])(_)).asInstanceOf[schema.Repr => A]
    }

  private def decode[A](record: Record[A])(v: record.Repr): A = {
    val size = v.getValues.size
    val vs = v.getValues
    val values = new Array[Any](size)
    var i = 0
    while (i < size) {
      val (_, schema) = record.schemas(i)
      val v = vs.get(i)
      values.update(i, dispatchDecode(schema)(v.asInstanceOf[schema.Repr]))
      i = i + 1
    }
    record.construct(values)
  }
  private def decode[A](schema: Type[A])(v: schema.Repr): A = v

  private def decode[A](schema: OptionType[A])(v: schema.Repr): Option[A] =
    Option(dispatchDecode(schema.schema)(v))

  private def decode[F[_], A: ClassTag](schema: ArrayType[F, A])(v: schema.Repr): F[A] = {
    val values = new Array[A](v.size)
    var i = 0
    while (i < v.size) {
      values.update(i, dispatchDecode[A](schema.schema)(v.get(i)))
      i = i + 1
    }
    schema.fromList(java.util.Arrays.asList(values: _*))
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

  private def decode[A](schema: Fallback[BCoder, A])(v: schema.Repr): A =
    CoderUtils.decodeFromByteArray(schema.coder, v)

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchEncode[A](schema: Schema[A], fieldType: BFieldType): schema.Encode =
    schema match {
      case s @ Record(_, _, _)    => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case RawRecord(_, _, toRow) => (toRow.apply _).asInstanceOf[A => schema.Repr]
      case s @ Type(_)            => (encode(s)(_)).asInstanceOf[A => schema.Repr]
      case s @ OptionType(_)      => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ ArrayType(_, _, _) =>
        (encode[s._F, s._T](s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ MapType(_, _, _, _) =>
        (encode[s._F, s._K, s._V](s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ Fallback(_) =>
        (encode(s.asInstanceOf[Fallback[BCoder, A]])(_)).asInstanceOf[A => schema.Repr]
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

  private def encode[A](schema: Type[A])(v: A): schema.Repr = v

  private def encode[A](schema: OptionType[A], fieldType: BFieldType)(v: Option[A]): schema.Repr =
    v.map { dispatchEncode(schema.schema, fieldType)(_) }
      .getOrElse(null.asInstanceOf[schema.Repr])

  private def encode[F[_], A](schema: ArrayType[F, A], fieldType: BFieldType)(
    v: F[A]): schema.Repr = {
    schema
      .toList(v)
      .asScala
      .map(dispatchEncode(schema.schema, fieldType.getCollectionElementType))
      .asJava
  }

  private def encode[F[_, _], A, B](schema: MapType[F, A, B], fieldType: BFieldType)(
    v: F[A, B]): schema.Repr = {
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

  private def encode[A](schema: Fallback[BCoder, A])(v: A): schema.Repr =
    CoderUtils.encodeToByteArray(schema.coder, v)

  final def materialize[T](
    sc: ScioContext,
    schema: Schema[T]): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) =
    materialize(sc.pipeline.getCoderRegistry, sc.options, schema)

  final def materializeWithDefault[T](
    schema: Schema[T]
  ): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) =
    materialize(CoderRegistry.createDefault(), PipelineOptionsFactory.create(), schema)

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
