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

import com.spotify.scio.coders.Coder
import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.{BeamSqlTable, SqlTransform}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.schemas.{SchemaCoder, Schema => BSchema}
import com.spotify.scio.util.ScioUtil
import org.apache.beam.sdk.extensions.sql.impl.rel.BeamRelNode
import org.apache.beam.sdk.extensions.sql.impl.{BeamSqlEnv, ParseException}
import org.apache.beam.sdk.extensions.sql.impl.schema.{BaseBeamTable, BeamPCollectionTable}
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils
import org.apache.beam.sdk.extensions.sql.meta.provider.{ReadOnlyTableProvider, TableProvider}

import scala.language.experimental.macros
import scala.util.Try
import scala.collection.JavaConverters._

object Sql {

  private[sql] val BeamProviderName = "beam"
  private[sql] val SCollectionTypeName = "SCOLLECTION"

  private[scio] def defaultTag[A]: TupleTag[A] = new TupleTag[A](SCollectionTypeName)

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

  private[sql] def tableProvider[A](tag: TupleTag[A], sc: SCollection[A]): TableProvider = {
    val table = new BeamPCollectionTable[A](sc.internal)
    new ReadOnlyTableProvider(SCollectionTypeName, Collections.singletonMap(tag.getId, table))
  }

  private[sql] def setSchema[T: Schema](c: SCollection[T]): SCollection[T] =
    c.transform { x =>
      val (schema, to, from) = SchemaMaterializer.materialize(c.context, Schema[T])
      x.map(identity)(Coder.beam(SchemaCoder.of(schema, to, from)))
    }
}

final class SqlSCollection[A: Schema](sc: SCollection[A]) {

  def query(q: String, udfs: Udf*): SCollection[Row] =
    query(Query[A, Row](q, Sql.defaultTag, udfs = udfs.toList))

  def query(q: Query[A, Row]): SCollection[Row] = {
    sc.context.wrap {
      val scWithSchema = Sql.setSchema(sc)
      val transform =
        SqlTransform
          .query(q.query)
          .withTableProvider(Sql.BeamProviderName, Sql.tableProvider(q.tag, scWithSchema))
      val sqlTransform = Sql.registerUdf(transform, q.udfs: _*)
      scWithSchema.applyInternal(sqlTransform)
    }
  }

  def queryAs[R: Schema](q: String, udfs: Udf*): SCollection[R] =
    queryAs(Query[A, R](q, Sql.defaultTag, udfs = udfs.toList))

  def queryAs[R: Schema](q: Query[A, R]): SCollection[R] =
    try {
      query(Query[A, Row](q.query, q.tag, q.udfs)).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Queries.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final class SqlSCollection2[A: Schema, B: Schema](a: SCollection[A], b: SCollection[B]) {

  def query(q: String, aTag: TupleTag[A], bTag: TupleTag[B], udfs: Udf*): SCollection[Row] =
    query(Query2(q, aTag, bTag, udfs.toList))

  def query(q: Query2[A, B, Row]): SCollection[Row] = {
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .apply(s"${collA.tfName} join ${collB.tfName}", sqlTransform)
    }
  }

  def queryAs[R: Schema](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query2(q, aTag, bTag, udfs.toList))

  def queryAs[R: Schema](q: Query2[A, B, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Queries.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}

final case class Query[A, B](
  query: String,
  tag: TupleTag[A] = Sql.defaultTag[A],
  udfs: List[Udf] = Nil
)

final case class Query2[A, B, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  udfs: List[Udf] = Nil
)

object Queries {

  /**
   * Typecheck [[Query]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[A: Schema, B: Schema](q: Query[A, B]): Either[String, Query[A, B]] =
    typecheck(
      q.query,
      List((q.tag.getId, SchemaMaterializer.beamSchema[A])),
      SchemaMaterializer.beamSchema[B],
      q.udfs
    ).right.map(_ => q)

  /**
   * Typecheck [[Query2]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[A: Schema, B: Schema, R: Schema](
    q: Query2[A, B, R]
  ): Either[String, Query2[A, B, R]] =
    typecheck(
      q.query,
      List(
        (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
        (q.bTag.getId, SchemaMaterializer.beamSchema[B])
      ),
      SchemaMaterializer.beamSchema[R],
      q.udfs
    ).right.map(_ => q)

  def typed[A: Schema, B: Schema](query: String): Query[A, B] =
    macro QueryMacros.typedImplDefaultTag[A, B]

  def typed[A: Schema, B: Schema](query: String, aTag: TupleTag[A]): Query[A, B] =
    macro QueryMacros.typedImpl[A, B]

  def typed[A: Schema, B: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B]
  ): Query2[A, B, R] =
    macro QueryMacros.typed2Impl[A, B, R]

  private[this] def parseQuery(
    query: String,
    schemas: List[(String, BSchema)],
    udfs: List[Udf]
  ): Try[BeamRelNode] = Try {
    val tables: Map[String, BeamSqlTable] = schemas.map {
      case (tag, schema) =>
        tag -> new BaseBeamTable(schema) {
          override def buildIOReader(begin: PBegin): PCollection[Row] = ???

          override def buildIOWriter(input: PCollection[Row]): POutput = ???

          override def isBounded: PCollection.IsBounded = PCollection.IsBounded.BOUNDED
        }
    }.toMap

    val env = BeamSqlEnv.readOnly(Sql.SCollectionTypeName, tables.asJava)
    udfs.foreach {
      case (x: UdfFromClass[_]) =>
        env.registerUdf(x.fnName, x.clazz)
      case (x: UdfFromSerializableFn[_, _]) =>
        env.registerUdf(x.fnName, x.fn)
      case (x: UdafFromCombineFn[_, _, _]) =>
        env.registerUdaf(x.fnName, x.fn)
    }
    env.parseQuery(query)
  }

  private[this] def schema(
    query: String,
    schemas: List[(String, BSchema)],
    udfs: List[Udf]
  ): Try[BSchema] =
    parseQuery(query, schemas, udfs).map(n => CalciteUtils.toSchema(n.getRowType))

  private[this] def printInferred(inferredSchemas: List[(String, BSchema)]): String =
    inferredSchemas
      .map {
        case (name, null) =>
          s"could not infer schema for $name"
        case (name, schema) =>
          s"""
          |schema of $name:
          |${PrettyPrint.prettyPrint(schema.getFields.asScala.toList)}
        """.stripMargin
      }
      .mkString("\n")

  private[this] def typecheck(
    query: String,
    inferredSchemas: List[(String, BSchema)],
    expectedSchema: BSchema,
    udfs: List[Udf]
  ): Either[String, String] = {
    ScioUtil
      .toEither(schema(query, inferredSchemas, udfs))
      .left
      .map { ex =>
        val mess = org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage(ex)

        s"""
           |$mess
           |
           |Query:
           |$query
           |
           |${printInferred(inferredSchemas)}
           |Query result schema (inferred) is unknown.
           |Expected schema:
           |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
      }
      .right
      .flatMap {
        case inferredSchema
            if SchemaTypes.equal(
              BSchema.FieldType.row(inferredSchema),
              BSchema.FieldType.row(expectedSchema)
            ) =>
          Right(query)
        case inferredSchema =>
          val message =
            s"""
               |Infered schema for query is not compatible with the expected schema.
               |
               |Query:
               |$query
               |
               |${printInferred(inferredSchemas)}
               |Query result schema (inferred):
               |${PrettyPrint.prettyPrint(inferredSchema.getFields.asScala.toList)}
               |
               |Expected schema:
               |${PrettyPrint.prettyPrint(expectedSchema.getFields.asScala.toList)}
        """.stripMargin
          Left(message)
      }
  }

}

object QueryMacros {
  import scala.reflect.macros.blackbox

  def typedImplDefaultTag[A: c.WeakTypeTag, B: c.WeakTypeTag](c: blackbox.Context)(
    query: c.Expr[String]
  )(iSchema: c.Expr[Schema[A]], oSchema: c.Expr[Schema[B]]): c.Expr[Query[A, B]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    val tag = c.Expr[TupleTag[A]](q"${Sql.defaultTag[A]}")
    typedImpl(c)(query, tag)(iSchema, oSchema)
  }

  def typedImpl[A: c.WeakTypeTag, B: c.WeakTypeTag](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]]
  )(iSchema: c.Expr[Schema[A]], oSchema: c.Expr[Schema[B]]): c.Expr[Query[A, B]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)

    val schemas: (Schema[A], Schema[B]) = c.eval(
      c.Expr(q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]})")
    )

    val sq = Query[A, B](cons(c)(query), tupleTag(c)(aTag))
    Queries
      .typecheck(sq)(schemas._1, schemas._2)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ => c.Expr[Query[A, B]](q"_root_.com.spotify.scio.sql.Query($query, $aTag)")
      )
  }

  def typed2Impl[A: c.WeakTypeTag, B: c.WeakTypeTag, R: c.WeakTypeTag](
    c: blackbox.Context
  )(query: c.Expr[String], aTag: c.Expr[TupleTag[A]], bTag: c.Expr[TupleTag[B]])(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    oSchema: c.Expr[Schema[R]]
  ): c.Expr[Query2[A, B, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[R](c)

    val schemas: (Schema[A], Schema[B], Schema[R]) = c.eval(
      c.Expr(q"(${inferImplicitSchema[A]}, ${inferImplicitSchema[B]}, ${inferImplicitSchema[R]})")
    )

    val sq = Query2[A, B, R](cons(c)(query), tupleTag(c)(aTag), tupleTag(c)(bTag))
    Queries
      .typecheck(sq)(schemas._1, schemas._2, schemas._3)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ => c.Expr[Query2[A, B, R]](q"_root_.com.spotify.scio.sql.Query2($query, $aTag, $bTag)")
      )
  }

  /**
   * Make sure that A is a concrete type bc. SQL macros can only
   * materialize Schema[A] is A is concrete
   */
  private[this] def assertConcrete[A: c.WeakTypeTag](c: blackbox.Context): Unit = {
    import c.universe._
    val wtt = weakTypeOf[A].dealias
    val isVal = wtt <:< typeOf[AnyVal]
    val isSealed =
      if (wtt.typeSymbol.isClass) {
        wtt.typeSymbol.asClass.isSealed
      } else false
    val isAbstract = wtt.typeSymbol.asType.isAbstract
    if (!isVal && isAbstract && !isSealed) {
      c.abort(c.enclosingPosition, s"$wtt is an abstract type, expected a concrete type.")
    } else {
      ()
    }
  }

  private[this] def cons[A](c: blackbox.Context)(e: c.Expr[String]): String = {
    import c.universe._
    e.tree match {
      case Literal(Constant(q: String)) => q
      case _ =>
        c.abort(c.enclosingPosition, s"Expression ${e.tree} does not evaluate to a constant")
    }
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
