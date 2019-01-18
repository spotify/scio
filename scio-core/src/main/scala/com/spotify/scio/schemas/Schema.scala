package com.spotify.scio.schemas

import com.spotify.scio.{IsJavaBean}
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import scala.language.higherKinds
import scala.reflect.{classTag, ClassTag}
import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.{Schema => BSchema}
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
final case class Type[T](fieldType: FieldType) extends Schema[T] {
  type Repr = T
}
final case class Optional[T](schema: Schema[T]) extends Schema[Option[T]] {
  type Repr = schema.Repr
}
final case class Fallback[F[_], T](coder: F[T]) extends Schema[T] {
  type Repr = Array[Byte]
}
final case class Arr[T](schema: Schema[T]) extends Schema[List[T]] { // TODO: polymorphism ?
  type Repr = java.util.List[schema.Repr]
}

private[schemas] case class ScalarWrapper[T](value: T) extends AnyVal

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
  implicit def arraySchema[T](implicit s: Schema[T]): Schema[List[T]] = Arr(s)

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: Schema[T] = {
    val schema =
      org.apache.beam.sdk.schemas.utils.JavaBeanUtils
        .schemaFromJavaBeanClass(classTag[T].runtimeClass)
    Type[T](FieldType.row(schema))
  }

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
      case Arr(s)      => FieldType.array(fieldType(s))
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
      case Arr(s) =>
        Arr(materializeSchema(sc, s))
    }

  import org.apache.beam.sdk.util.CoderUtils

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchDecode[A](schema: Schema[A]): schema.Decode =
    schema match {
      case s @ Record(_, _, _) => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Type(_)         => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Optional(_)     => (decode(s)(_)).asInstanceOf[schema.Repr => A]
      case s @ Arr(_)          => (decode(s)(_)).asInstanceOf[schema.Repr => A]
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
  private def decode[A: ClassTag](schema: Arr[A])(v: schema.Repr): List[A] =
    v.asScala.map { v =>
      dispatchDecode[A](schema.schema)(v)
    }.toList
  private def decode[A](schema: Fallback[BCoder, A])(v: schema.Repr): A =
    CoderUtils.decodeFromByteArray(schema.coder, v)

  // XXX: scalac can't unify schema.Repr with s.Repr
  private def dispatchEncode[A](schema: Schema[A], fieldType: FieldType): schema.Encode =
    schema match {
      case s @ Record(_, _, _) => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ Type(_)         => (encode(s)(_)).asInstanceOf[A => schema.Repr]
      case s @ Optional(_)     => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
      case s @ Arr(_)          => (encode(s, fieldType)(_)).asInstanceOf[A => schema.Repr]
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
  private def encode[A: ClassTag](schema: Arr[A], fieldType: FieldType)(v: List[A]): schema.Repr =
    v.map { dispatchEncode(schema.schema, fieldType.getCollectionElementType)(_) }.asJava
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
        val (bschema, to, from) = materialize(sc, implicitly[Record[ScalarWrapper[T]]])
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
