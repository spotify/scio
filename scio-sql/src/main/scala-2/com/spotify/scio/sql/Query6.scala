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

final case class Query6[A, B, C, D, E, F, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  udfs: List[Udf] = Nil
)

object Query6 {
  import scala.reflect.macros.blackbox
  import QueryMacros._

  def typecheck[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, R: Schema](
    q: Query6[A, B, C, D, E, F, R]
  ): Either[String, Query6[A, B, C, D, E, F, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, E: Schema, F: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F]
  ): Query6[A, B, C, D, E, F, R] =
    macro typed6Impl[A, B, C, D, E, F, R]

  def typed6Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query6[A, B, C, D, E, F, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[R](c)

    val (schemas1, schemas2, schemas3, schemas4, schemas5, schemas6, schemas7) =
      c.eval(
        c.Expr[(Schema[A], Schema[B], Schema[C], Schema[D], Schema[E], Schema[F], Schema[R])](
          q"(${untyped(aSchema)}, ${untyped(bSchema)}, ${untyped(cSchema)}, ${untyped(dSchema)}, ${untyped(
            eSchema
          )}, ${untyped(fSchema)}, ${untyped(rSchema)})"
        )
      )

    val sq = Query6[A, B, C, D, E, F, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag)
    )
    typecheck(sq)(schemas1, schemas2, schemas3, schemas4, schemas5, schemas6, schemas7)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query6[A, B, C, D, E, F, R]](
            q"_root_.com.spotify.scio.sql.Query6($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag)"
          )
      )
  }
}

final class SqlSCollection6[
  A: Schema: ClassTag,
  B: Schema: ClassTag,
  C: Schema: ClassTag,
  D: Schema: ClassTag,
  E: Schema: ClassTag,
  F: Schema: ClassTag
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F]
) {

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query6(q, aTag, bTag, cTag, dTag, eTag, fTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(q: Query6[A, B, C, D, E, F, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .apply(
          s"${collA.tfName} join ${collB.tfName} join ${collC.tfName} join ${collD.tfName} join ${collE.tfName} join ${collF.tfName}",
          sqlTransform
        )

    }

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def queryAs[R: Schema: ClassTag](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query6(q, aTag, bTag, cTag, dTag, eTag, fTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def queryAs[R: Schema: ClassTag](q: Query6[A, B, C, D, E, F, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.fTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query6.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
