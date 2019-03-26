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

import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.{BeamSqlTable, SqlTransform}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
import org.apache.beam.sdk.extensions.sql.impl.{BeamSqlEnv, ParseException}
import org.apache.beam.sdk.extensions.sql.impl.schema.BaseBeamTable
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils

import scala.language.experimental.macros
import scala.util.Try
import scala.collection.JavaConverters._

final case class Query[A, B](query: String, udfs: List[Udf] = Nil)

final case class Query2[A, B, R](query: String,
                                 aTag: TupleTag[A],
                                 bTag: TupleTag[B],
                                 udfs: List[Udf] = Nil)

object Queries {

  /**
   * Typecheck [[Query]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[A: Schema, B: Schema](q: Query[A, B]): Either[String, Query[A, B]] = {
    val schema: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val expectedSchema: BSchema =
      Schema[B] match {
        case s: Record[B] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[B]]).getRowSchema
      }

    QueryUtils.typecheck(q.query, schema, expectedSchema).right.map(_ => q)
  }

  /**
   * Typecheck [[Query2]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[A: Schema, B: Schema, R: Schema](
    q: Query2[A, B, R]): Either[String, Query2[A, B, R]] = {
    val schemaA: BSchema = SchemaMaterializer.fieldType(Schema[A]).getRowSchema
    val schemaB: BSchema = SchemaMaterializer.fieldType(Schema[B]).getRowSchema
    val expectedSchema: BSchema =
      Schema[R] match {
        case s: Record[R] =>
          SchemaMaterializer.fieldType(s).getRowSchema
        case _ =>
          SchemaMaterializer.fieldType(Schema[ScalarWrapper[R]]).getRowSchema
      }

    QueryUtils
      .typecheck(q.query, List((q.aTag.getId, schemaA), (q.bTag.getId, schemaB)), expectedSchema)
      .right
      .map(_ => q)
  }

  def typed[A: Schema, B: Schema](query: String, udfs: Udf*): Query[A, B] =
    macro QueryMacros.typedImpl[A, B]

  def typed[A: Schema, B: Schema, R: Schema](query: String,
                                             aTag: TupleTag[A],
                                             bTag: TupleTag[B],
                                             udfs: Udf*): Query2[A, B, R] =
    macro QueryMacros.typed2Impl[A, B, R]
}

object Sql {

  def from[A: Schema](sc: SCollection[A]): SqlSCollection[A] = new SqlSCollection(sc)

  def from[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]): SqlSCollection2[A, B] =
    new SqlSCollection2(a, b)

  private[sql] def registerUdf(t: SqlTransform, udfs: Udf*): SqlTransform =
    udfs.foldLeft(t) {
      case (st, x: UdfFromClass[_]) =>
        st.registerUdf(x.fnName, x.clazz)
      case (st, x: UdfFromSerializableFn[_, _]) =>
        st.registerUdf(x.fnName, x.fn)
      case (st, x: UdafFromCombineFn[_, _, _]) =>
        st.registerUdaf(x.fnName, x.fn)
    }

}

final class SqlSCollection[A: Schema](sc: SCollection[A]) {

  def queryRaw(q: String, udfs: Udf*): SCollection[Row] = query(Query(q, udfs.toList))

  def query(q: Query[A, Row]): SCollection[Row] = {
    sc.context.wrap {
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)
      QueryUtils.transform(sc).applyInternal(sqlTransform)
    }
  }

  def queryRawAs[R: Schema](q: String, udfs: Udf*): SCollection[R] =
    queryAs(Query(q, udfs.toList))

  def queryAs[R: Schema](q: Query[A, R]): SCollection[R] =
    try {
      val (schema, to, from) = SchemaMaterializer.materialize(sc.context, Schema[R])
      queryRaw(q.query, q.udfs: _*)
        .map[R](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    } catch {
      case e: ParseException =>
        Queries.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final class SqlSCollection2[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]) {

  def queryRaw(q: String, aTag: TupleTag[A], bTag: TupleTag[B], udfs: Udf*): SCollection[Row] =
    query(Query2(q, aTag, bTag, udfs.toList))

  def query(q: Query2[A, B, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = QueryUtils.transform(a)
      val collB = QueryUtils.transform(b)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .apply(s"${collA.tfName} join ${collB.tfName}", sqlTransform)
    }
  }

  def queryRawAs[R: Schema](q: String,
                            aTag: TupleTag[A],
                            bTag: TupleTag[B],
                            udfs: Udf*): SCollection[R] =
    queryAs(Query2(q, aTag, bTag, udfs.toList))

  def queryAs[R: Schema](q: Query2[A, B, R]): SCollection[R] =
    try {
      val (schema, to, from) = SchemaMaterializer.materialize(a.context, Schema[R])
      queryRaw(q.query, q.aTag, q.bTag, q.udfs: _*)
        .map[R](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
    } catch {
      case e: ParseException =>
        Queries.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

object QueryUtils {

  private[this] val PCollectionName = "PCOLLECTION"

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

    BeamSqlEnv.readOnly(PCollectionName, tables.asJava).parseQuery(query)
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

  def typedImpl[A, B](c: blackbox.Context)(query: c.Expr[String], udfs: c.Expr[Udf]*)(
    iSchema: c.Expr[Schema[A]],
    oSchema: c.Expr[Schema[B]]): c.Expr[Query[A, B]] = {
    import c.universe._

    val queryTree = c.untypecheck(query.tree.duplicate)
    val sInTree = c.untypecheck(iSchema.tree.duplicate)
    val sOutTree = c.untypecheck(oSchema.tree.duplicate)

    val (sIn, sOut) =
      c.eval(c.Expr[(Schema[A], Schema[B])](q"($sInTree, $sOutTree)"))

    val sq =
      queryTree match {
        case Literal(Constant(q: String)) =>
          Query[A, B](q)
        case _ =>
          c.abort(c.enclosingPosition, s"Expression $queryTree does not evaluate to a constant")
      }

    Queries
      .typecheck(sq)(sIn, sOut)
      .fold(
        err => c.abort(c.enclosingPosition, err), { t =>
          c.Expr[Query[A, B]](q"_root_.com.spotify.scio.sql.Query($query, ..$udfs)")
        }
      )
  }

  def typed2Impl[A, B, R](c: blackbox.Context)(query: c.Expr[String],
                                               aTag: c.Expr[TupleTag[A]],
                                               bTag: c.Expr[TupleTag[B]],
                                               udfs: c.Expr[Udf]*)(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    oSchema: c.Expr[Schema[R]]): c.Expr[Query2[A, B, R]] = {
    import c.universe._

    val queryTree = c.untypecheck(query.tree.duplicate)
    val sInTreeA = c.untypecheck(aSchema.tree.duplicate)
    val sInTreeB = c.untypecheck(bSchema.tree.duplicate)
    val sOutTree = c.untypecheck(oSchema.tree.duplicate)

    val (sInA, sInB, sOut) =
      c.eval(c.Expr[(Schema[A], Schema[B], Schema[R])](q"($sInTreeA, $sInTreeB, $sOutTree)"))

    val sq =
      queryTree match {
        case Literal(Constant(q: String)) =>
          Query2[A, B, R](q, tupleTag(c)(aTag), tupleTag(c)(bTag))
        case _ =>
          c.abort(c.enclosingPosition, s"Expression $queryTree does not evaluate to a constant")
      }

    Queries
      .typecheck(sq)(sInA, sInB, sOut)
      .fold(
        err => c.abort(c.enclosingPosition, err), { t =>
          val out = q"_root_.com.spotify.scio.sql.Query2($query, $aTag, $bTag, ..$udfs)"
          c.Expr[Query2[A, B, R]](out)
        }
      )
  }

  private[this] def tupleTag[T](c: blackbox.Context)(e: c.Expr[TupleTag[T]]): TupleTag[T] = {
    import c.universe._

    e.tree match {
      case Apply(_, List(Literal(Constant(tag: String)))) => new TupleTag[T](tag)
      case _ =>
        c.abort(c.enclosingPosition, s"Expression ${e.tree}")
    }
  }

}
