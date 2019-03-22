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

import com.spotify.scio.values._
import com.spotify.scio.coders._
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.sql.syntax.sqltransform._
import com.spotify.scio.schemas.{
  PrettyPrint,
  Record,
  ScalarWrapper,
  Schema,
  SchemaMaterializer,
  SchemaTypes
}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.extensions.sql.{BeamSqlTable, SqlTransform}
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
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

  /**
   * Typecheck [[Query]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[I: Schema, O: Schema](q: Query[I, O]): Either[String, Query[I, O]] = {
    val schema: BSchema = SchemaMaterializer.fieldType(Schema[I]).getRowSchema
    val expectedSchema: BSchema =
      Schema[O] match {
        case s: Record[O] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[O]]).getRowSchema
      }

    QueryUtils.typecheck(q.query, schema, expectedSchema).map(_ => q)
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
        val sqlTransform = SqlTransform.query(query).registerUdf(udfs: _*)
        QueryUtils.transform(c).applyInternal(sqlTransform)
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

      override def apply(s: SCollection[I]): SCollection[O] = {
        try {
          val (schema, to, from) = SchemaMaterializer.materialize(s.context, Schema[O])
          row[I](query, udfs: _*)
            .apply(s)
            .map[O](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
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

trait Query2[A, B, C] extends ((SCollection[A], SCollection[B]) => SCollection[C]) {
  def query: String
}

object Query2 {

  def typecheck[A: Schema, B: Schema, O: Schema](q: Query2[A, B, O],
                                                 left: String,
                                                 right: String): Either[String, Query2[A, B, O]] = {
    val schemaA: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val schemaB: BSchema = SchemaMaterializer.fieldType(Schema[B]).getRowSchema
    val expectedSchema: BSchema =
      Schema[O] match {
        case s: Record[O] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[O]]).getRowSchema
      }

    QueryUtils
      .typecheck(q.query, List((left, schemaA), (right, schemaB)), expectedSchema)
      .map(_ => q)
  }

  def row[A: Schema, B: Schema](q: String,
                                left: String,
                                right: String,
                                udfs: Udf*): Query2[A, B, Row] =
    new Query2[A, B, Row] {
      override def query: String = q

      override def apply(a: SCollection[A], b: SCollection[B]): SCollection[Row] =
        a.context.wrap {
          val collA = QueryUtils.transform(a)
          val collB = QueryUtils.transform(b)
          val sqlTransform = SqlTransform.query(query).registerUdf(udfs: _*)

          PCollectionTuple
            .of(new TupleTag[A](left), collA.internal)
            .and(new TupleTag[B](right), collB.internal)
            .apply(s"${collA.tfName} join ${collB.tfName}", sqlTransform)
        }
    }

  def of[A: Schema, B: Schema, O: Schema](q: String,
                                          left: String,
                                          right: String,
                                          udfs: Udf*): Query2[A, B, O] =
    new Query2[A, B, O] {
      val query: String = q

      def apply(a: SCollection[A], b: SCollection[B]): SCollection[O] = {
        try {
          val (schema, to, from) = SchemaMaterializer.materialize(a.context, Schema[O])
          row[A, B](query, left, right, udfs: _*)
            .apply(a, b)
            .map[O](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
        } catch {
          case e: ParseException =>
            typecheck(this, left, right)
              .fold(err => throw new RuntimeException(err, e), _ => throw e)
        }
      }
    }

  def tsql[A: Schema, B: Schema, O: Schema](query: String,
                                            left: String,
                                            right: String,
                                            udfs: Udf*): Query2[A, B, O] =
    macro QueryMacros.tsqlJoinImpl[A, B, O]
}

object QueryUtils {

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

  def transform[T: Schema](c: SCollection[T]): SCollection[T] = {
    val coderT: Coder[T] = {
      val (schema, to, from) = SchemaMaterializer.materialize(c.context, Schema[T])
      Coder.beam(SchemaCoder.of(schema, to, from))
    }
    c.transform(s"${c.tfName}: set schema")(_.map(identity)(coderT))
  }

  def parseQuery(query: String, schemas: (String, BSchema)*): Try[BeamRelNode] = Try {
    val tables: Map[String, BeamSqlTable] = schemas.map {
      case (tag, schema) =>
        tag -> new BaseBeamTable(schema) {
          override def buildIOReader(begin: PBegin): PCollection[Row] = ???

          override def buildIOWriter(input: PCollection[Row]): POutput = ???

          override def isBounded: PCollection.IsBounded = PCollection.IsBounded.BOUNDED
        }
    }.toMap

    silence { () =>
      BeamSqlEnv.readOnly(PCollectionName, tables.asJava).parseQuery(query)
    }
  }

  def parseQuery(query: String, schema: BSchema): Try[BeamRelNode] =
    parseQuery(query, (PCollectionName, schema))

  def schema(query: String, schemas: (String, BSchema)*): Try[BSchema] =
    parseQuery(query, schemas: _*).map(n => CalciteUtils.toSchema(n.getRowType))

  def schema(query: String, s: BSchema): Try[BSchema] = schema(query, (PCollectionName, s))

  def typecheck(query: String,
                inferredSchema: BSchema,
                expectedSchema: BSchema): Either[String, String] =
    typecheck(query, List((PCollectionName, inferredSchema)), expectedSchema)

  def typecheck(query: String,
                inferredSchemas: List[(String, BSchema)],
                expectedSchema: BSchema): Either[String, String] = {
    ScioUtil
      .toEither(schema(query, inferredSchemas: _*))
      .left
      .map { ex =>
        val mess = org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage(ex)
        s"""
           |$mess
           |
           |Query:
           |$query
           |
           |PCOLLECTION schema:
           |${inferredSchemas.map(i => PrettyPrint.prettyPrint(i._2.getFields.asScala.toList))}
           |Query result schema (infered) is unknown
           |Expected schema:
           |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
      }
      .right
      .flatMap {
        case inferredSchema
            if SchemaTypes.typesEqual(BSchema.FieldType.row(inferredSchema),
                                      BSchema.FieldType.row(expectedSchema)) =>
          Right(query)
        case inferredSchema =>
          val message =
            s"""
               |Infered schema for query is not compatible with the expected schema.
               |
               |Query:
               |$query
               |
               |PCOLLECTION schema:
               |${inferredSchemas.map(i => PrettyPrint.prettyPrint(i._2.getFields.asScala.toList))}
               |Query result schema (infered):
               |${PrettyPrint.prettyPrint(inferredSchema.getFields.asScala.toList)}
               |Expected schema:
               |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
          Left(message)
      }
  }
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

  def tsqlJoinImpl[A, B, O](c: blackbox.Context)(query: c.Expr[String],
                                                 left: c.Expr[String],
                                                 right: c.Expr[String],
                                                 udfs: c.Expr[Udf]*)(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    oSchema: c.Expr[Schema[O]]): c.Expr[Query2[A, B, O]] = {
    import c.universe._

    val queryTree = c.untypecheck(query.tree.duplicate)
    val sInTreeA = c.untypecheck(aSchema.tree.duplicate)
    val sInTreeB = c.untypecheck(bSchema.tree.duplicate)
    val sOutTree = c.untypecheck(oSchema.tree.duplicate)

    val leftName = c.eval(left)
    val rightName = c.eval(right)
    val (sInA, sInB, sOut) =
      c.eval(c.Expr[(Schema[A], Schema[B], Schema[O])](q"($sInTreeA, $sInTreeB, $sOutTree)"))

    val sq =
      queryTree match {
        case Literal(Constant(q: String)) =>
          Query2.of(q, leftName, rightName)(sInA, sInB, sOut)
        case _ =>
          c.abort(c.enclosingPosition, s"Expression $queryTree does not evaluate to a constant")
      }

    Query2
      .typecheck(sq, leftName, rightName)(sInA, sInB, sOut)
      .fold(
        err => c.abort(c.enclosingPosition, err), { t =>
          val out =
            q"_root_.com.spotify.scio.sql.Query.of($query, $left, $right, ..$udfs)($aSchema, $bSchema, $oSchema)"
          c.Expr[Query2[A, B, O]](out)
        }
      )
  }
}
