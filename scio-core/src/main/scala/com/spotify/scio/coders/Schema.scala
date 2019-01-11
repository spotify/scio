package com.spotify.scio.coders

import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import BSchema.{Field, FieldType}

sealed trait Schema[T]
final case class FSchema[T](fieldType: FieldType) extends Schema[T]
final case class OSchema[T](s: Schema[T]) extends Schema[Option[T]]

object Schema extends LowPrioritySchemaDerivation {
  implicit def stringSchema = FSchema[String](FieldType.STRING)
  implicit def byteSchema = FSchema[Byte](FieldType.BYTE)
  implicit def bytesSchema = FSchema[Array[Byte]](FieldType.BYTES)
  implicit def sortSchema = FSchema[Short](FieldType.INT16)
  implicit def intSchema = FSchema[Int](FieldType.INT32)
  implicit def LongSchema = FSchema[Long](FieldType.INT64)
  implicit def floatSchema = FSchema[Float](FieldType.FLOAT)
  implicit def doubleSchema = FSchema[Double](FieldType.DOUBLE)
  implicit def bigdecimalSchema = FSchema[BigDecimal](FieldType.DECIMAL)
  implicit def booleanSchema = FSchema[Boolean](FieldType.BOOLEAN)
  // implicit def datetimeSchema = FSchema[](FieldType.DATETIME)

  implicit def optionSchema[T](implicit s: Schema[T]): Schema[Option[T]] = OSchema(s)

  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  implicit def fallback[A](implicit lp: shapeless.LowPriority) =
    FSchema[A](FieldType.BYTES) // ¯\_(ツ)_/¯

  // TODO: List and Map Schemas
  // TODO: Schema for traits ?
}

trait LowPrioritySchemaDerivation {
  import language.experimental.macros, magnolia._

  private def materialize[T](name: String, s: Schema[T]): Field =
    s match {
      case FSchema(ft) =>
        Field.of(name, ft)
      case OSchema(s) =>
        materialize(name, s).withNullable(true)
    }

  type Typeclass[T] = Schema[T]

  def combine[T](ctx: CaseClass[Schema, T]): Schema[T] = {
    val fields =
      ctx.parameters.map { p =>
        materialize(p.label, p.typeclass)
      }
    val schema = new BSchema(fields.asJava)
    FSchema[T](FieldType.row(schema))
  }

  implicit def gen[T]: Schema[T] = macro Magnolia.gen[T]
}
