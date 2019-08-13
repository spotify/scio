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
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{Schema => BSchema, SchemaCoder}
import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.annotation.tailrec
import scala.tools.reflect.ToolBox

sealed trait To[I, O] extends (SCollection[I] => SCollection[O])

object To {

  @tailrec @inline
  private def getBaseType(t: BSchema.FieldType): BSchema.FieldType = {
    val log = t.getLogicalType()
    if (log == null) t
    else getBaseType(log.getBaseType())
  }

  /**
   * Test if Rows with Schema t0 can be safely converted to Rows with Schema t1
   */
  private def areCompatible(t0: BSchema.FieldType, t1: BSchema.FieldType): Boolean = {
    (t0.getTypeName, t1.getTypeName, t0.getNullable == t1.getNullable) match {
      case (_, _, false) =>
        false
      case (BSchema.TypeName.ROW, BSchema.TypeName.ROW, _) =>
        areCompatible(t0.getRowSchema, t1.getRowSchema)
      case (BSchema.TypeName.ARRAY, BSchema.TypeName.ARRAY, _) =>
        areCompatible(t0.getCollectionElementType, t1.getCollectionElementType)
      case (BSchema.TypeName.MAP, BSchema.TypeName.MAP, _) =>
        areCompatible(t0.getMapKeyType, t1.getMapKeyType) &&
          areCompatible(t0.getMapValueType, t1.getMapValueType)
      case (_, _, _) =>
        t0.equivalent(t1, BSchema.EquivalenceNullablePolicy.SAME)
    }
  }

  private def areCompatible(s0: BSchema, s1: BSchema): Boolean = {
    val s0Fields = s0.getFields.asScala.map { x =>
      (x.getName, x)
    }.toMap
    s1.getFields.asScala.forall { f =>
      s0Fields
        .get(f.getName)
        .exists(other => areCompatible(f.getType, other.getType))
    }
  }

  def checkCompatibility[T](bsi: BSchema, bso: BSchema)(
    t: => T
  ): Either[String, T] =
    if (areCompatible(bsi, bso)) {
      Right(t)
    } else {
      val message =
        s"""
        |Schema are not compatible.
        |
        |FROM schema:
        |${PrettyPrint.prettyPrint(bsi.getFields.asScala.toList)}
        |TO schema:
        |${PrettyPrint.prettyPrint(bso.getFields.asScala.toList)}""".stripMargin
      Left(message)
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
  def unsafe[I: Schema, O: Schema]: To[I, O] = unsafe(unchecked)

  private[scio] def unsafe[I: Schema, O: Schema](to: To[I, O]): To[I, O] =
    new To[I, O] {
      def apply(coll: SCollection[I]): SCollection[O] = {
        val (bst, _, _) = SchemaMaterializer.materialize(coll.context, Schema[I])
        val (bso, _, _) = SchemaMaterializer.materialize(coll.context, Schema[O])

        checkCompatibility(bst, bso)(to)
          .fold(message => throw new IllegalArgumentException(message), _.apply(coll))
      }
    }

  /**
   * FOR INTERNAL USE ONLY - Convert from I to O without performing any check.
   * @see To#safe
   * @see To#unsafe
   */
  def unchecked[I: Schema, O: Schema]: To[I, O] =
    new To[I, O] {
      def apply(coll: SCollection[I]): SCollection[O] = {
        val (_, toT, _) = SchemaMaterializer.materialize(coll.context, Schema[I])
        val convertRow: (BSchema, I) => Row = { (s, i) =>
          val row = toT(i)
          transform(s)(row)
        }
        unchecked[I, O](convertRow).apply(coll)
      }
    }

  private[scio] def unchecked[I, O: Schema](f: (BSchema, I) => Row): To[I, O] =
    new To[I, O] {
      def apply(coll: SCollection[I]): SCollection[O] = {
        val (bso, toO, fromO) = SchemaMaterializer.materialize(coll.context, Schema[O])
        val convert: I => O = f.curried(bso).andThen(fromO(_))
        coll.map[O](convert)(Coder.beam(SchemaCoder.of(bso, toO, fromO)))
      }
    }

  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes. The compatibility of thoses classes is checked
   * at compile time.
   * @see To#unsafe
   */
  def safe[I: Schema, O: Schema]: To[I, O] =
    macro ToMacro.safeImpl[I, O]
}

import scala.reflect.macros._
import reflect.runtime.{universe => u}

private[scio] final class FastEval(evalToolBox: ToolBox[u.type]) {

  def eval[T](ctx: blackbox.Context)(expr: ctx.Expr[T]): T = {
    import ctx.universe._

    //scalastyle:off
    val evalImporter =
      u.internal
        .createImporter(ctx.universe)
        .asInstanceOf[u.Importer { val from: ctx.universe.type }]
    //scalastyle:on

    expr.tree match {
      case Literal(Constant(value)) =>
        ctx.eval[T](expr)
      case _ =>
        val imported = evalImporter.importTree(expr.tree)
        evalToolBox.eval(imported).asInstanceOf[T]
    }
  }
}

/**
 * Provide faster evaluation of tree by reusing the same toolbox
 * This is faster bc. resusing the same toolbox also meas reusing the same classloader,
 * which saves disks IOs since we don't load the same classes from disk multiple times
 */
private[scio] final object FastEval {
  import scala.tools.reflect._

  private lazy val tl: ToolBox[u.type] = {
    val evalMirror = scala.reflect.runtime.currentMirror
    evalMirror.mkToolBox()
  }

  def apply[T](ctx: blackbox.Context)(expr: ctx.Expr[T]): T =
    new FastEval(tl).eval(ctx)(expr)
}

object ToMacro {
  import scala.reflect.macros._
  def safeImpl[I: c.WeakTypeTag, O: c.WeakTypeTag](
    c: blackbox.Context
  )(iSchema: c.Expr[Schema[I]], oSchema: c.Expr[Schema[O]]): c.Expr[To[I, O]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    val tpeI = weakTypeOf[I]
    val tpeO = weakTypeOf[O]

    val expr = c.Expr[(Schema[I], Schema[O])](q"(${untyped(iSchema)}, ${untyped(oSchema)})")
    val (sIn, sOut) = FastEval(c)(expr)

    val schemaOut: BSchema = SchemaMaterializer.fieldType(sOut).getRowSchema()
    val schemaIn: BSchema = SchemaMaterializer.fieldType(sIn).getRowSchema()

    To.checkCompatibility(schemaIn, schemaOut) {
        q"""_root_.com.spotify.scio.schemas.To.unchecked[$tpeI, $tpeO]"""
      }
      .fold(message => c.abort(c.enclosingPosition, message), t => c.Expr[To[I, O]](t))
  }

}
