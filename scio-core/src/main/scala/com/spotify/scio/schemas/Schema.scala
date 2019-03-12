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

import com.spotify.scio.coders.Coder
import com.spotify.scio.IsJavaBean
import scala.language.higherKinds
import scala.reflect.{classTag, ClassTag}
import scala.collection.JavaConverters._
import org.apache.beam.sdk.schemas.{
  Schema => BSchema,
  JavaBeanSchema,
  AvroRecordSchema,
  SchemaProvider
}
import org.apache.beam.sdk.schemas.utils.AvroUtils
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{Row, TypeDescriptor}
import org.apache.avro.specific.SpecificRecord
import BSchema.FieldType

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

final case class RawRecord[T](schema: BSchema,
                              fromRow: SerializableFunction[Row, T],
                              toRow: SerializableFunction[T, Row])
    extends Schema[T] {
  type Repr = Row
}

object RawRecord {
  def apply[T: ClassTag](provider: SchemaProvider): RawRecord[T] = {
    val rc = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    val td = TypeDescriptor.of(rc)
    val schema = provider.schemaFor(td)
    def toRow = provider.toRowFunction(td)
    def fromRow = provider.fromRowFunction(td)
    RawRecord(schema, fromRow, toRow)
  }

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

  implicit def javaBeanSchema[T: IsJavaBean: ClassTag]: RawRecord[T] =
    RawRecord[T](new JavaBeanSchema())

  private class SerializableSchema(@transient schema: org.apache.avro.Schema) extends Serializable {
    val stringSchema = schema.toString
    def get: org.apache.avro.Schema = new org.apache.avro.Schema.Parser().parse(stringSchema)
  }

  // Workaround BEAM-6742
  private def specificRecordtoRow[T <: SpecificRecord](schema: BSchema,
                                                       avroSchema: SerializableSchema,
                                                       t: T): Row = {
    val row = Row.withSchema(schema)
    schema.getFields.asScala.zip(avroSchema.get.getFields.asScala).zipWithIndex.foreach {
      case ((f, a), i) =>
        val value = t.get(i)
        val v = AvroUtils.convertAvroFieldStrict(value, a.schema, f.getType)
        row.addValue(v)
    }
    row.build()
  }

  implicit def avroSchema[T <: SpecificRecord: ClassTag]: Schema[T] = {
    // TODO: broken because of a bug upstream https://issues.apache.org/jira/browse/BEAM-6742
    // RawRecord[T](new AvroRecordSchema())
    import org.apache.avro.reflect.ReflectData
    val rc = classTag[T].runtimeClass.asInstanceOf[Class[T]]
    val provider = new AvroRecordSchema()
    val td = TypeDescriptor.of(rc)
    val schema = provider.schemaFor(td)
    val avroSchema = new SerializableSchema(ReflectData.get().getSchema(td.getRawType()))

    def fromRow = provider.fromRowFunction(td)

    val toRow: SerializableFunction[T, Row] =
      new SerializableFunction[T, Row] {
        def apply(t: T): Row =
          specificRecordtoRow(schema, avroSchema, t)
      }
    RawRecord[T](schema, fromRow, toRow)
  }

  @inline final def apply[T](implicit c: Schema[T]): Schema[T] = c

  // TODO: List and Map Schemas
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

private[scio] object PrettyPrint {
  private def printContent(fs: List[BSchema.Field], prefix: String = ""): String = {
    fs.map { f =>
        val nullable = if (f.getType.getNullable) "YES" else "NO"
        // val out =  s"${prefix}${f.getName}\t${f.getType.getTypeName}\t$nullable\n"
        val `type` = f.getType
        val typename =
          `type`.getTypeName match {
            case t @ BSchema.TypeName.ARRAY =>
              s"${`type`.getCollectionElementType.getTypeName}[]"
            case t => t
          }
        val out =
          f"│ ${prefix + f.getName}%-40s │ ${typename}%-8s │ $nullable%-8s │%n"
        val underlying =
          if (f.getType.getTypeName == BSchema.TypeName.ROW)
            printContent(f.getType.getRowSchema.getFields.asScala.toList, s"${prefix}${f.getName}.")
          else ""

        out + underlying
      }
      .mkString("")
  }

  def prettyPrint(fs: List[BSchema.Field]): String = {
    val header =
      f"""
      |┌──────────────────────────────────────────┬──────────┬──────────┐
      |│ NAME                                     │ TYPE     │ NULLABLE │
      |├──────────────────────────────────────────┼──────────┼──────────┤%n""".stripMargin.drop(1)
    val footer =
      f"""
      |└──────────────────────────────────────────┴──────────┴──────────┘%n""".stripMargin.trim

    header + printContent(fs) + footer
  }
}
