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
package com.spotify.scio.sql

import com.spotify.scio.values._
import com.spotify.scio.coders._
import com.spotify.scio.schemas.{PrettyPrint, Record, ScalarWrapper, Schema, SchemaMaterializer}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable
import org.apache.beam.sdk.schemas.{Schema => BSchema}
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils
import com.google.common.collect.ImmutableMap

import scala.collection.JavaConverters._
import scala.language.experimental.macros

// TODO: could be a PTransform
sealed trait Query[I, O] extends (SCollection[I] => SCollection[O]) {
  val query: String
}

object Query {

  val PCOLLECTION_NAME = "PCOLLECTION"

  // Beam is annoyingly verbose when is parses SQL queries.
  // This function makes is silent.
  private def silence[A](a: Unit => A): A = {
    val prop = "org.slf4j.simpleLogger.defaultLogLevel"
    val ll = System.getProperty(prop)
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "ERROR")
    val x = a(())
    System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", ll)
    x
  }

  def typecheck[I: Schema, O: Schema](q: Query[I, O]): Either[String, Query[I, O]] = {
    val schema: BSchema = SchemaMaterializer.fieldType(Schema[I]).getRowSchema()

    val table = new BaseBeamTable(schema) {
      def buildIOReader(begin: PBegin): PCollection[Row] = ???
      def buildIOWriter(input: PCollection[Row]): POutput = ???
      def isBounded(): PCollection.IsBounded = PCollection.IsBounded.BOUNDED
    }

    val sqlEnv =
      BeamSqlEnv.readOnly(Query.PCOLLECTION_NAME, ImmutableMap.of(Query.PCOLLECTION_NAME, table))

    val expectedSchema: BSchema =
      Schema[O] match {
        case s @ Record(_, _, _) =>
          SchemaMaterializer.fieldType(s).getRowSchema()
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[O]]).getRowSchema()
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
        case _ if (s1.getNullable == s2.getNullable) => true
        case _                                       => false
      })

    scala.util
      .Try(silence(_ => sqlEnv.parseQuery(q.query)))
      .toEither
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
      .map { q =>
        CalciteUtils.toSchema(q.getRowType)
      }
      .flatMap {
        case inferedSchema
            if typesEqual(BSchema.FieldType.row(inferedSchema),
                          BSchema.FieldType.row(expectedSchema)) =>
          Right(q)
        case inferedSchema =>
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
          |${PrettyPrint.prettyPrint(inferedSchema.getFields.asScala.toList)}
          |Expected schema:
          |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
          Left(message)
      }
  }

  def row[I: Schema](q: String, udfs: Udf*): Query[I, Row] =
    new Query[I, Row] {
      val query = q
      def apply(c: SCollection[I]) = {
        val scoll = c.setSchema(Schema[I])
        val sqlEnv = BeamSqlEnv.readOnly(
          PCOLLECTION_NAME,
          ImmutableMap.of(PCOLLECTION_NAME, new BeamPCollectionTable(scoll.internal)))
        var sqlTransform = SqlTransform.query(query)

        udfs.foreach {
          case x: UdfFromClass[_] =>
            sqlTransform = sqlTransform.registerUdf(x.fnName, x.clazz)
            sqlEnv.registerUdf(x.fnName, x.clazz)
          case x: UdfFromSerializableFn[_, _] =>
            sqlTransform = sqlTransform.registerUdf(x.fnName, x.fn)
            sqlEnv.registerUdf(x.fnName, x.fn)
          case x: UdafFromCombineFn[_, _, _] =>
            sqlTransform = sqlTransform.registerUdaf(x.fnName, x.fn)
            sqlEnv.registerUdaf(x.fnName, x.fn)
        }

        val q = sqlEnv.parseQuery(query)
        val schema = CalciteUtils.toSchema(q.getRowType)

        scoll.applyTransform[Row](sqlTransform)(Coder.row(schema))
      }
    }

  def of[I: Schema, O: Schema](q: String, udfs: Udf*): Query[I, O] =
    new Query[I, O] {
      val query = q
      def apply(s: SCollection[I]): SCollection[O] = {
        try {
          import org.apache.beam.sdk.schemas.SchemaCoder
          val (schema, to, from) = SchemaMaterializer.materialize(s.context, Schema[O])
          val coll: SCollection[Row] = Query.row[I](query, udfs: _*).apply(s)
          coll.map[O](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
        } catch {
          case e: org.apache.beam.sdk.extensions.sql.impl.ParseException =>
            Query
              .typecheck(this)
              .fold(err => throw new RuntimeException(err, e), _ => throw e)
        }
      }
    }

  def tsql[I: Schema, O: Schema](query: String, udfs: Udf*): Query[I, O] =
    macro com.spotify.scio.sql.QueryMacros.tsqlImpl[I, O]

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
