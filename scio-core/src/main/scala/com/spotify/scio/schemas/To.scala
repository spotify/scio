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

import com.spotify.scio.values._
import com.spotify.scio.coders._
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{Schema => BSchema, SchemaCoder}
import scala.collection.JavaConverters._
import scala.language.experimental.macros

sealed trait To[I, O] extends (SCollection[I] => SCollection[O])

object To {
  private def areCompatible(t0: BSchema.FieldType, t1: BSchema.FieldType): Boolean = {
    (t0.getTypeName, t1.getTypeName, t0.getNullable == t1.getNullable) match {
      case (_, _, false) =>
        false
      case (BSchema.TypeName.ROW, BSchema.TypeName.ROW, _) =>
        areCompatible(t0.getRowSchema, t1.getRowSchema)
      case (BSchema.TypeName.ARRAY, BSchema.TypeName.ARRAY, _) =>
        areCompatible(t0.getCollectionElementType, t1.getCollectionElementType)
      case (BSchema.TypeName.MAP, BSchema.TypeName.MAP, _) =>
        areCompatible(t0.getMapKeyType, t1.getMapKeyType)
        areCompatible(t0.getMapValueType, t1.getMapValueType)
      case (_, _, _) =>
        t0.equivalent(t1, BSchema.EquivalenceNullablePolicy.SAME)
    }
  }

  private def areCompatible(s0: BSchema, s1: BSchema): Boolean = {
    val s0Fields = s0.getFields.asScala
    s1.getFields.asScala.forall { f =>
      s0Fields
        .find { x =>
          x.getName == f.getName
        }
        .map { other =>
          val res = areCompatible(f.getType, other.getType)
          res
        }
        .getOrElse {
          false
        }
    }
  }

  private[schemas] def checkCompatibility[T](bsi: BSchema, bso: BSchema)(
    t: => T): Either[String, T] = {
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
  }

  @inline private def transform(schema: BSchema): Row => Row = { t0 =>
    val values =
      schema.getFields.asScala.map { f =>
        t0.getValue[Object](f.getName) match {
          case None => null
          case r if f.getType.getTypeName == BSchema.TypeName.ROW =>
            transform(f.getType.getRowSchema)(r.asInstanceOf[Row])
          case v =>
            v
        }
      }
    Row
      .withSchema(schema)
      .addValues(values: _*)
      .build()
  }

  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes.
   * The compatibility of the 2 schemas is NOT checked at compile time, so the execution may fail.
   * @see To#apply
   */
  def unsafe[I: Schema, O: Schema]: To[I, O] =
    new To[I, O] {
      def apply(coll: SCollection[I]): SCollection[O] = {
        val (bst, toT, _) = SchemaMaterializer.materialize(coll.context, Schema[I])
        val (bso, toO, fromO) = SchemaMaterializer.materialize(coll.context, Schema[O])

        checkCompatibility(bst, bso) {
          val trans = transform(bso)
          coll.map[O] { t =>
            fromO(trans(toT(t)))
          }(Coder.beam(SchemaCoder.of(bso, toO, fromO)))
        }.fold(message => throw new IllegalArgumentException(message), identity)
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
        val (bst, toT, _) = SchemaMaterializer.materialize(coll.context, Schema[I])
        val (bso, toO, fromO) = SchemaMaterializer.materialize(coll.context, Schema[O])
        val trans = transform(bso)
        coll.map[O] { t =>
          fromO(trans(toT(t)))
        }(Coder.beam(SchemaCoder.of(bso, toO, fromO)))
      }
    }

  /**
   * Convert instance of ${T} in this SCollection into instances of ${O}
   * based on the Schemas on the 2 classes. The compatibility of thoses classes is checked
   * at compile time.
   * @see To#unsafe
   */
  def safe[I: Schema, O: Schema]: To[I, O] =
    macro com.spotify.scio.schemas.ToMacro.safeImpl[I, O]
}

object ToMacro {
  import scala.reflect.macros.blackbox
  def safeImpl[I: c.WeakTypeTag, O: c.WeakTypeTag](c: blackbox.Context)(
    iSchema: c.Expr[Schema[I]],
    oSchema: c.Expr[Schema[O]]): c.Expr[To[I, O]] = {
    import c.universe._

    val tpeI = weakTypeOf[I]
    val tpeO = weakTypeOf[O]

    val sInTree = c.untypecheck(iSchema.tree.duplicate)
    val sOutTree = c.untypecheck(oSchema.tree.duplicate)

    val (sIn, sOut) =
      c.eval(c.Expr[(Schema[I], Schema[O])](q"($sInTree, $sOutTree)"))

    val schemaIn: BSchema = SchemaMaterializer.fieldType(sIn).getRowSchema()
    val schemaOut: BSchema = SchemaMaterializer.fieldType(sOut).getRowSchema()

    To.checkCompatibility(schemaIn, schemaOut) {
        q"""_root_.com.spotify.scio.schemas.To.unchecked[$tpeI, $tpeO]"""
      }
      .fold(message => c.abort(c.enclosingPosition, message), t => c.Expr[To[I, O]](t))
  }
}
