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

// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
// !! generated with sql.py
// !! DO NOT EDIT MANUALLY
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

package com.spotify.scio.sql

import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.ParseException
import org.apache.beam.sdk.values._

import scala.reflect.ClassTag

final case class Query5[A, B, C, D, E, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  udfs: List[Udf] = Nil
)

object Query5 {
  import scala.reflect.macros.blackbox
  import QueryMacros._

  def typecheck[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, R: Schema](
    q: Query5[A, B, C, D, E, R]
  ): Either[String, Query5[A, B, C, D, E, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E]
  ): Query5[A, B, C, D, E, R] =
    macro typed5Impl[A, B, C, D, E, R]

  def typed5Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query5[A, B, C, D, E, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[R](c)

    val (schemas1, schemas2, schemas3, schemas4, schemas5, schemas6) =
      c.eval(
        c.Expr[(Schema[A], Schema[B], Schema[C], Schema[D], Schema[E], Schema[R])](
          q"(${untyped(aSchema)}, ${untyped(bSchema)}, ${untyped(cSchema)}, ${untyped(dSchema)}, ${untyped(eSchema)}, ${untyped(rSchema)})"
        )
      )

    val sq = Query5[A, B, C, D, E, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag)
    )
    typecheck(sq)(schemas1, schemas2, schemas3, schemas4, schemas5, schemas6)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query5[A, B, C, D, E, R]](
            q"_root_.com.spotify.scio.sql.Query5($query, $aTag, $bTag, $cTag, $dTag, $eTag)"
          )
      )
  }
}

final class SqlSCollection5[
  A: Schema: ClassTag,
  B: Schema: ClassTag,
  C: Schema: ClassTag,
  D: Schema: ClassTag,
  E: Schema: ClassTag
](a: SCollection[A], b: SCollection[B], c: SCollection[C], d: SCollection[D], e: SCollection[E]) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query5(q, aTag, bTag, cTag, dTag, eTag, udfs.toList))

  def query(q: Query5[A, B, C, D, E, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .apply(
          s"${collA.tfName} join ${collB.tfName} join ${collC.tfName} join ${collD.tfName} join ${collE.tfName}",
          sqlTransform
        )

    }

  def queryAs[R: Schema: ClassTag](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query5(q, aTag, bTag, cTag, dTag, eTag, udfs.toList))

  def queryAs[R: Schema: ClassTag](q: Query5[A, B, C, D, E, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query5.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
