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
package com.spotify.scio.sql

import java.util.Collections

import com.spotify.scio.values._
import com.spotify.scio.coders._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.schemas.{PrettyPrint, Record, ScalarWrapper, Schema, SchemaMaterializer}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.{BeamSqlEnv, ParseException}
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.util.Try

sealed trait Query[I, O] extends (SCollection[I] => SCollection[O]) {
  def query: String
}

object Query {

  private[this] val PCollectionName = "PCOLLECTION"

  // Beam is annoyingly verbose when is parses SQL queries.
  // This function makes is silent.
  private def silence[A](a: () => A): A = {
    val prop = "org.slf4j.simpleLogger.defaultLogLevel"
    val ll = System.getProperty(prop)
    System.setProperty(prop, "ERROR")
    val x = a()
    if (ll != null) System.setProperty(prop, ll)
    x
  }

  /**
   * Typecheck [[Query]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[I: Schema, O: Schema](q: Query[I, O]): Either[String, Query[I, O]] = {
    val schema: BSchema = SchemaMaterializer.fieldType(Schema[I]).getRowSchema

    val table = new BaseBeamTable(schema) {
      override def buildIOReader(begin: PBegin): PCollection[Row] = ???

      override def buildIOWriter(input: PCollection[Row]): POutput = ???

      override def isBounded: PCollection.IsBounded = PCollection.IsBounded.BOUNDED
    }

    val sqlEnv =
      BeamSqlEnv.readOnly(PCollectionName, Collections.singletonMap(PCollectionName, table))

    val expectedSchema: BSchema =
      Schema[O] match {
        case s: Record[O] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[O]]).getRowSchema
      }

    def typesEqual(s1: BSchema.FieldType, s2: BSchema.FieldType): Boolean =
      (s1.getTypeName == s2.getTypeName) && (s1.getTypeName match {
        case BSchema.TypeName.ROW =>
          s1.getRowSchema.getFields.asScala
            .map(_.getType)
            .zip(s2.getRowSchema.getFields.asScala.map(_.getType))
            .forall { case (l, r) => typesEqual(l, r) }
        case BSchema.TypeName.ARRAY =>
          typesEqual(s1.getCollectionElementType, s2.getCollectionElementType)
        case BSchema.TypeName.MAP =>
          typesEqual(s1.getMapKeyType, s2.getMapKeyType) && typesEqual(s1.getMapValueType,
                                                                       s2.getMapValueType)
        case _ if s1.getNullable == s2.getNullable => true
        case _                                     => false
      })

    ScioUtil
      .toEither(Try(silence(() => sqlEnv.parseQuery(q.query))))
      .left
      .map { ex =>
        val mess = org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage(ex)
        s"""
          |$mess
          |
          |Query:
          |${q.query}
          |
          |PCOLLECTION schema:
          |${PrettyPrint.prettyPrint(schema.getFields.asScala.toList)}
          |Query result schema (infered) is unknown
          |Expected schema:
          |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
      }
      .right
      .map { q =>
        CalciteUtils.toSchema(q.getRowType)
      }
      .right
      .flatMap {
        case inferredSchema
            if typesEqual(BSchema.FieldType.row(inferredSchema),
                          BSchema.FieldType.row(expectedSchema)) =>
          Right(q)
        case inferredSchema =>
          val message =
            s"""
          |Infered schema for query is not compatible with the expected schema.
          |
          |Query:
          |${q.query}
          |
          |PCOLLECTION schema:
          |${PrettyPrint.prettyPrint(schema.getFields.asScala.toList)}
          |Query result schema (infered):
          |${PrettyPrint.prettyPrint(inferredSchema.getFields.asScala.toList)}
          |Expected schema:
          |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
          Left(message)
      }
  }

  /**
   * Create a BeanSQL Query that can be applied on a SCollection[I]
   * and will return a SCollection[Row].
   * Note that the Schema of thoses Rows is automatically infered, and therefore,
   * does not need to be set.
   */
  def row[I: Schema](q: String, udfs: Udf*): Query[I, Row] =
    new Query[I, Row] {
      val query: String = q
      def apply(c: SCollection[I]): SCollection[Row] = c.context.wrap {

        // XXX: Hack to set the coder on the existing PCOLLECTION
        // and hide it in a "set schema" dataflow block
        val coder = {
          val (schema, to, from) = SchemaMaterializer.materialize(c.context, Schema[I])
          Coder.beam(SchemaCoder.of(schema, to, from))
        }
        val scoll = c.transform(s"${c.tfName}: set schema")(_.map(identity)(coder))
        val sqlTransform = udfs.foldLeft(SqlTransform.query(query)) {
          case (st, x: UdfFromClass[_]) =>
            st.registerUdf(x.fnName, x.clazz)
          case (st, x: UdfFromSerializableFn[_, _]) =>
            st.registerUdf(x.fnName, x.fn)
          case (st, x: UdafFromCombineFn[_, _, _]) =>
            st.registerUdaf(x.fnName, x.fn)
        }

        scoll.applyInternal(sqlTransform)
      }
    }

  /**
   * Create a BeanSQL Query that can be applied on a SCollection[I]
   * and will return a SCollection[O].
   * The Schema of O is expected to match the data returned by the SQL query.
   * If it does not, a RuntimeException will be thrown.
   */
  def of[I: Schema, O: Schema](q: String, udfs: Udf*): Query[I, O] =
    new Query[I, O] {
      val query: String = q

      def apply(s: SCollection[I]): SCollection[O] = {
        try {
          val (schema, to, from) = SchemaMaterializer.materialize(s.context, Schema[O])
          val coll: SCollection[Row] = Query.row[I](query, udfs: _*).apply(s)
          coll.map[O](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
        } catch {
          case e: ParseException =>
            Query
              .typecheck(this)
              .fold(err => throw new RuntimeException(err, e), _ => throw e)
        }
      }
    }

  /**
   * Similar to [[Query.of]] exept the query is type-checked at compile time.
   * @note [[query]] needs to be a stable (known at compile time) value.
   * @see Query.of
   */
  def tsql[I: Schema, O: Schema](query: String, udfs: Udf*): Query[I, O] =
    macro QueryMacros.tsqlImpl[I, O]

}

object QueryMacros {
  import scala.reflect.macros.blackbox

  def tsqlImpl[I, O](c: blackbox.Context)(query: c.Expr[String], udfs: c.Expr[Udf]*)(
    iSchema: c.Expr[Schema[I]],
    oSchema: c.Expr[Schema[O]]): c.Expr[Query[I, O]] = {
    import c.universe._

    val queryTree = c.untypecheck(query.tree.duplicate)
    val sInTree = c.untypecheck(iSchema.tree.duplicate)
    val sOutTree = c.untypecheck(oSchema.tree.duplicate)

    val (sIn, sOut) =
      c.eval(c.Expr[(Schema[I], Schema[O])](q"($sInTree, $sOutTree)"))

    val sq =
      queryTree match {
        case Literal(Constant(q: String)) =>
          Query.of(q)(sIn, sOut)
        case _ =>
          c.abort(c.enclosingPosition, s"Expression $queryTree does not evaluate to a constant")
      }

    Query
      .typecheck(sq)(sIn, sOut)
      .fold(
        err => c.abort(c.enclosingPosition, err), { t =>
          val out =
            q"_root_.com.spotify.scio.sql.Query.of($query, ..$udfs)($iSchema, $oSchema)"
          c.Expr[Query[I, O]](out)
        }
      )
  }
}
