package com.spotify.scio.schemas

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import scala.language.higherKinds
import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.coders.{Coder => BCoder}
import org.apache.beam.sdk.values.Row
import BSchema.{Field, FieldType}

sealed trait Schema[T]
final case class Record[T] private (schemas: Array[(String, Schema[Any])],
                                    construct: Seq[Any] => T,
                                    destruct: T => Array[Any])
    extends Schema[T]
final case class Type[T](fieldType: FieldType) extends Schema[T]
final case class Optional[T](s: Schema[T]) extends Schema[Option[T]]
final case class Fallback[F[_], T](coder: F[T]) extends Schema[T]

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

  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  // TODO: List and Map Schemas
  // TODO: Schema for traits ?
}

trait LowPriorityFallbackSchema extends LowPrioritySchemaDerivation {
  def fallback[A: Coder]: Schema[A] =
    Fallback[Coder, A](Coder[A]) // ¯\_(ツ)_/¯
}

private object Derived extends Serializable {
  import magnolia._
  def combineSchema[T](ps: Seq[Param[Schema, T]], rawConstruct: Seq[Any] => T) = {
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

  implicit def gen[T]: Record[T] = macro Magnolia.gen[T]
}

object SchemaMaterializer {

  import com.spotify.scio.ScioContext

  @inline private def fieldType[A](schema: Schema[A]): FieldType =
    schema match {
      case Record(schemas, _, _) =>
        val fields =
          schemas.map {
            case (name, schema) =>
              // cast to workaround a bug in scalac
              // (possibly https://github.com/scala/bug/issues/10195 ?)
              (name, schema.asInstanceOf[Schema[_]]) match {
                case (name, Optional(schema)) =>
                  Field.of(name, fieldType(schema)).withNullable(true)
                case (name, schema) =>
                  Field.of(name, fieldType(schema))
              }
          }

        FieldType.row(BSchema.of(fields: _*))
      case Type(t)     => t
      case Fallback(_) => FieldType.BYTES
      case Optional(_) => throw new IllegalStateException("Optional(_) match should be impossible")
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
    }

  import org.apache.beam.sdk.util.CoderUtils
  // TODO: typesafety
  private def decode[A](schema: Schema[A], v: Any): A =
    schema match {
      case Record(schemas, construct, destruct) =>
        val row = v.asInstanceOf[Row]
        val values =
          row.getValues.asScala.zip(schemas).map {
            case (v, (name, schema)) =>
              decode(schema, v)
          }
        construct(values)
      case Type(t) =>
        v.asInstanceOf[A]
      case Optional(s) =>
        Option(decode(s, v))
      case Fallback(c) =>
        CoderUtils.decodeFromByteArray(c.asInstanceOf[BCoder[A]], v.asInstanceOf[Array[Byte]])
    }

  // TODO: typesafety
  private def encode[A](schema: Schema[A], fieldType: FieldType, v: A): Any =
    schema match {
      case Record(schemas, construct, destruct) =>
        val builder = Row.withSchema(fieldType.getRowSchema())
        destruct(v)
          .zip(schemas)
          .zip(fieldType.getRowSchema.getFields.asScala)
          .map {
            case ((v, (_, s)), f) =>
              encode(s, f.getType, v)
          }
          .foreach(builder.addValue _)
        builder.build()
      case Type(t) =>
        v
      case Optional(s) =>
        Option(encode(s, fieldType, v))
      case Fallback(c) =>
        CoderUtils.encodeToByteArray(c.asInstanceOf[BCoder[A]], v)
    }

  def materialize[T](
    sc: ScioContext,
    schema: Schema[T]): (BSchema, SerializableFunction[T, Row], SerializableFunction[Row, T]) = {
    //  XXX: Is getRowSchema safe ?
    // TODO: check for null values ?
    val ft = fieldType(schema)
    val bschema = ft.getRowSchema()
    val schemaMat = materializeSchema(sc, schema)

    def fromRow =
      new SerializableFunction[Row, T] {
        def apply(r: Row): T =
          decode[T](schemaMat, r)
      }

    def toRow =
      new SerializableFunction[T, Row] {
        def apply(t: T): Row =
          encode[T](schemaMat, ft, t).asInstanceOf[Row]
      }

    (bschema, toRow, fromRow)
  }
}
