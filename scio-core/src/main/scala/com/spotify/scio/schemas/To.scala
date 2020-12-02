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

import com.spotify.scio.values._
import com.spotify.scio.coders._
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}

import scala.jdk.CollectionConverters._
import scala.annotation.tailrec
import scala.reflect.ClassTag

sealed trait To[I, O] extends (SCollection[I] => SCollection[O]) with Serializable {
  def coder: Coder[O]
  def convert(i: I): O

  final def apply(coll: SCollection[I]): SCollection[O] =
    coll.map(i => convert(i))(coder)
}

object To extends ToMacro {

  @tailrec @inline
  private def getBaseType(t: BSchema.FieldType): BSchema.FieldType = {
    val log = t.getLogicalType()
    if (log == null) t
    else getBaseType(log.getBaseType())
  }

  // Position API
  private case class Location(p: List[String])
  private case class Positional[T](location: Location, value: T)

  private def merge(p0: Location, p1: Location): Location =
    Location(p0.p ++ p1.p)

  sealed trait Nullable
  case object `NOT NULLABLE` extends Nullable
  case object NULLABLE extends Nullable
  private object NullableBuilder {
    def fromBoolean(isNullable: Boolean): Nullable =
      if (isNullable) NULLABLE else `NOT NULLABLE`
  }

  // Error API
  sealed private trait Error
  private case class NullableError(got: Nullable, expected: Nullable) extends Error
  private case class TypeError(got: BSchema.FieldType, expected: BSchema.FieldType) extends Error
  private case object FieldNotFound extends Error

  private type Errors = List[Positional[Error]]

  /** Test if Rows with Schema t0 can be safely converted to Rows with Schema t1 */
  private def areCompatible(
    context: Location
  )(t0: BSchema.FieldType, t1: BSchema.FieldType): Errors =
    (t0.getTypeName, t1.getTypeName, t0.getNullable == t1.getNullable) match {
      case (_, _, false) =>
        val expected = NullableBuilder.fromBoolean(t1.getNullable())
        val got = NullableBuilder.fromBoolean(t0.getNullable())
        List(Positional(context, NullableError(got, expected)))
      case (BSchema.TypeName.ROW, BSchema.TypeName.ROW, _) =>
        areCompatible(t0.getRowSchema, t1.getRowSchema).map { e =>
          Positional(merge(context, e.location), e.value)
        }
      case (BSchema.TypeName.ARRAY, BSchema.TypeName.ARRAY, _) =>
        areCompatible(context)(t0.getCollectionElementType, t1.getCollectionElementType)
      case (BSchema.TypeName.MAP, BSchema.TypeName.MAP, _) =>
        areCompatible(context)(t0.getMapKeyType, t1.getMapKeyType) ++
          areCompatible(context)(t0.getMapValueType, t1.getMapValueType)
      case (_, _, _) =>
        if (t0.equivalent(t1, BSchema.EquivalenceNullablePolicy.SAME)) Nil
        else List(Positional(context, TypeError(t0, t1)))
    }

  private def areCompatible(s0: BSchema, s1: BSchema): Errors = {
    val s0Fields =
      s0.getFields.asScala.map(x => (x.getName, x)).toMap

    s1.getFields.asScala.toList.flatMap { f =>
      val name = f.getName
      val loc = Location(List(name))
      s0Fields
        .get(name)
        .map(other => areCompatible(loc)(other.getType, f.getType))
        .getOrElse[Errors](List(Positional(loc, FieldNotFound)))
    }
  }

  private def mkPath(p: Location): String = p.p.mkString(".")

  private def messageFor(err: Positional[Error]): String =
    err match {
      case Positional(p, NullableError(got, expected)) =>
        s"⨯ Field ${mkPath(p)} has incompatible NULLABLE. Got: $got expected: $expected"
      case Positional(p, TypeError(got, expected)) =>
        s"⨯ Field ${mkPath(p)} has incompatible types." +
          s" Got: ${got.getTypeName} expected: ${expected.getTypeName}"
      case Positional(p, FieldNotFound) => s"⨯ Field ${mkPath(p)} was not found"
    }

  def checkCompatibility[T](bsi: BSchema, bso: BSchema)(
    t: => T
  ): Either[String, T] = {
    val errors = areCompatible(bsi, bso)
    if (errors.isEmpty) {
      Right(t)
    } else {
      val message =
        s"""
        |Schema are not compatible.
        |
        |FROM schema:
        |${PrettyPrint.prettyPrint(bsi.getFields.asScala.toList)}
        |TO schema:
        |${PrettyPrint.prettyPrint(bso.getFields.asScala.toList)}
        |""".stripMargin
      val errorsMsg = errors.map(messageFor).mkString("\n")
      Left(message ++ errorsMsg)
    }
  }

  /**
   * Builds a function that reads a Row and convert it
   * to a Row in the given Schema.
   * The input Row needs to be compatible with the given Schema,
   * that is, it may have more fields, or use LogicalTypes.
   */
  @inline private def transform(schema: BSchema): Row => Row = { t0 =>
    val iter = schema.getFields.iterator()
    val builder: Row.Builder = Row.withSchema(schema)
    while (iter.hasNext) {
      val f = iter.next()
      val value = t0.getValue[Object](f.getName) match {
        case None => null
        case r: Row if f.getType.getTypeName == BSchema.TypeName.ROW =>
          transform(f.getType.getRowSchema)(r)
        case v =>
          // See comment in `SchemaMaterializer.decode` implementation
          // for an explanation on why this is required.
          SchemaMaterializer.decode(Type(f.getType()))(v)
      }
      builder.addValue(value)
    }

    builder.build()
  }

  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes.
   * The compatibility of the 2 schemas is NOT checked at compile time, so the execution may fail.
   * @see To#apply
   */
  def unsafe[I: Schema, O: Schema: ClassTag]: To[I, O] = unsafe(unchecked)

  private[scio] def unsafe[I: Schema, O: Schema](to: To[I, O]): To[I, O] =
    new To[I, O] {
      val (bst, _, _) = SchemaMaterializer.materialize(Schema[I])
      val (bso, _, _) = SchemaMaterializer.materialize(Schema[O])
      val underlying: To[I, O] = checkCompatibility(bst, bso)(to)
        .fold(message => throw new IllegalArgumentException(message), identity)

      val coder = underlying.coder
      def convert(i: I): O = underlying.convert(i)
    }

  /**
   * FOR INTERNAL USE ONLY - Convert from I to O without performing any check.
   * @see To#safe
   * @see To#unsafe
   */
  def unchecked[I: Schema, O: Schema: ClassTag]: To[I, O] =
    new To[I, O] {
      val (_, toT, _) = SchemaMaterializer.materialize(Schema[I])
      val convertRow: (BSchema, I) => Row = { (s, i) =>
        val row = toT(i)
        transform(s)(row)
      }
      val underlying = unchecked[I, O](convertRow)

      val coder = underlying.coder
      def convert(i: I): O = underlying.convert(i)
    }

  private[scio] def unchecked[I, O: Schema: ClassTag](f: (BSchema, I) => Row): To[I, O] =
    new To[I, O] {
      val (bso, toO, fromO) = SchemaMaterializer.materialize(Schema[O])
      val td = TypeDescriptor.of(ScioUtil.classOf[O])
      val coder = Coder.beam(SchemaCoder.of(bso, td, toO, fromO))
      def convert(i: I): O = f.curried(bso).andThen(fromO(_))(i)
    }
}
