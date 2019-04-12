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

package com.spotify.scio

import com.spotify.scio.values.SCollection
import com.spotify.scio.schemas.Schema
import org.apache.beam.sdk.values.TupleTag

import scala.language.implicitConversions
import scala.language.experimental.macros

package object sql {

  trait SQLBuilder {
    def as[B: Schema]: SCollection[B]
  }

  sealed trait SqlParam
  final case class SCollectionRef[A: Schema](coll: SCollection[A]) extends SqlParam {
    type _A = A
    val schema = Schema[A]
  }
  final case class UdfRef(udf: Udf) extends SqlParam

  implicit def toUdfRef(udf: Udf): SqlParam = new UdfRef(udf)
  implicit def toSCollectionRef[A: Schema](coll: SCollection[A]): SqlParam =
    new SCollectionRef[A](coll)

  final implicit class SqlInterpolator(val sc: StringContext) extends AnyVal {

    // TODO: typed SQL ?
    // TODO: at least 1 params is a SCollectionRef
    def sql(p0: SqlParam, ps: SqlParam*): SQLBuilder = {
      val params = p0 :: ps.toList
      val tags =
        params.zipWithIndex.collect {
          case (ref @ SCollectionRef(scoll), i) =>
            (scoll.name, (ref, new TupleTag[ref._A](s"SCOLLECTION_$i")))
        }.toMap

      val udfs =
        params.collect {
          case UdfRef(u) => u
        }

      val strings = sc.parts.iterator
      val expressions = params.iterator
      var buf = new StringBuffer(strings.next)
      while (strings.hasNext) {
        val param = expressions.next
        val p =
          param match {
            case SCollectionRef(scoll) =>
              tags(scoll.name)._2.getId
            case UdfRef(udf) =>
              udf.fnName
          }
        buf.append(p)
        buf.append(strings.next)
      }
      val q = buf.toString

      tags.toList match {
        case (name, (ref, tag)) :: Nil =>
          new SQLBuilder {
            def as[B: Schema] =
              Sql
                .from(ref.coll)(ref.schema)
                .queryAs(new Query[ref._A, B](q, tag, udfs))
          }
        case (name0, (ref0, tag0)) :: (name1, (ref1, tag1)) :: Nil =>
          new SQLBuilder {
            def as[B: Schema] =
              Sql
                .from(ref0.coll, ref1.coll)(ref0.schema, ref1.schema)
                .queryAs(new Query2[ref0._A, ref1._A, B](q, tag0, tag1, udfs))
          }
        case _ =>
          throw new IllegalArgumentException("WUUUUTTT????")
      }
    }

    def tsql[A, B](p0: SCollection[A]): SCollection[B] =
      macro SqlInterpolatorMacro.tsqlImpl[A, B]
  }

}

package sql {
  import scala.reflect.macros.whitebox

  private sealed trait SqlInterpolatorMacroHelpers {
    protected val ctx: whitebox.Context

    import ctx.universe._

    def buildSQLString: String = {

      val parts = ctx.prefix.tree match {
        case q"com.spotify.scio.sql.`package`.SqlInterpolator(scala.StringContext.apply(..$ps))" =>
          ps
        case q"sql.this.`package`.SqlInterpolator(scala.StringContext.apply(..$ps))" => ps
        case tree =>
          ctx.abort(ctx.enclosingPosition,
                    s"Implementation error. Expected tsql string interpolation, found $tree")
      }

      parts
        .map {
          case Literal(Constant(s: String)) => s
          case tree =>
            ctx.abort(ctx.enclosingPosition,
                      s"Implementation error. Expected Literal(Constant(...)), found $tree")
        }
        .mkString(Sql.SCollectionTypeName)
    }

    def inferImplicitSchemas[A: ctx.WeakTypeTag, B: ctx.WeakTypeTag]
      : (ctx.Tree, ctx.Tree, Schema[A], Schema[B]) = {
      val wttA = ctx.weakTypeTag[A]
      val wttB = ctx.weakTypeTag[B]
      val needA = ctx.typecheck(tq"_root_.com.spotify.scio.schemas.Schema[$wttA]", ctx.TYPEmode).tpe
      val needB = ctx.typecheck(tq"_root_.com.spotify.scio.schemas.Schema[$wttB]", ctx.TYPEmode).tpe

      val sa = ctx.inferImplicitValue(needA)
      val sb = ctx.inferImplicitValue(needB)

      val schemaATree = ctx.untypecheck(sa.duplicate)
      val schemaBTree = ctx.untypecheck(sb.duplicate)

      val (scha, schab) =
        ctx.eval(ctx.Expr[(Schema[A], Schema[B])](q"($schemaATree, $schemaBTree)"))
      (sa, sb, scha, schab)
    }
  }

  object SqlInterpolatorMacro {

    def tsqlImpl[A: c.WeakTypeTag, B: c.WeakTypeTag](c: whitebox.Context)(
      p0: c.Expr[SCollection[A]]): c.Expr[SCollection[B]] = {
      val h = new SqlInterpolatorMacroHelpers { val ctx = c }
      import h._
      import c.universe._

      val wttA = c.weakTypeTag[A]
      val wttB = c.weakTypeTag[B]

      val sql = buildSQLString
      val (implA, implB, sIn, sOut) = inferImplicitSchemas[A, B]

      val query = Query[A, B](sql, new TupleTag[A](Sql.SCollectionTypeName))

      def q =
        c.Expr[Query[A, B]](q"""
        _root_.com.spotify.scio.sql.Query[$wttA, $wttB](
          $sql,
          new _root_.org.apache.beam.sdk.values.TupleTag[$wttA](
            _root_.com.spotify.scio.sql.Sql.SCollectionTypeName))
        """)

      def tree =
        c.Expr[SCollection[B]](q"""
          _root_.com.spotify.scio.sql.Sql
              .from($p0)(${implA.asInstanceOf[c.Tree]})
              .queryAs($q)(${implB.asInstanceOf[c.Tree]})
        """)

      Queries
        .typecheck(query)(sIn, sOut)
        .fold(err => c.abort(c.enclosingPosition, err), _ => tree)
    }
  }
}
