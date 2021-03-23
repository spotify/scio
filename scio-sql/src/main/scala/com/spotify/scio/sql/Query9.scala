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

final case class Query9[A, B, C, D, E, F, G, H, I, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  gTag: TupleTag[G],
  hTag: TupleTag[H],
  iTag: TupleTag[I],
  udfs: List[Udf] = Nil
)

object Query9 {
  import scala.reflect.macros.blackbox
  import QueryMacros._

  def typecheck[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    R: Schema
  ](q: Query9[A, B, C, D, E, F, G, H, I, R]): Either[String, Query9[A, B, C, D, E, F, G, H, I, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D]),
          (q.eTag.getId, SchemaMaterializer.beamSchema[E]),
          (q.fTag.getId, SchemaMaterializer.beamSchema[F]),
          (q.gTag.getId, SchemaMaterializer.beamSchema[G]),
          (q.hTag.getId, SchemaMaterializer.beamSchema[H]),
          (q.iTag.getId, SchemaMaterializer.beamSchema[I])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    I: Schema,
    R: Schema
  ](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    eTag: TupleTag[E],
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I]
  ): Query9[A, B, C, D, E, F, G, H, I, R] =
    macro typed9Impl[A, B, C, D, E, F, G, H, I, R]

  def typed9Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    E: c.WeakTypeTag,
    F: c.WeakTypeTag,
    G: c.WeakTypeTag,
    H: c.WeakTypeTag,
    I: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]],
    eTag: c.Expr[TupleTag[E]],
    fTag: c.Expr[TupleTag[F]],
    gTag: c.Expr[TupleTag[G]],
    hTag: c.Expr[TupleTag[H]],
    iTag: c.Expr[TupleTag[I]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    eSchema: c.Expr[Schema[E]],
    fSchema: c.Expr[Schema[F]],
    gSchema: c.Expr[Schema[G]],
    hSchema: c.Expr[Schema[H]],
    iSchema: c.Expr[Schema[I]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query9[A, B, C, D, E, F, G, H, I, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[E](c)
    assertConcrete[F](c)
    assertConcrete[G](c)
    assertConcrete[H](c)
    assertConcrete[I](c)
    assertConcrete[R](c)

    val (
      schemas1,
      schemas2,
      schemas3,
      schemas4,
      schemas5,
      schemas6,
      schemas7,
      schemas8,
      schemas9,
      schemas10
    ) =
      c.eval(
        c.Expr[
          (
            Schema[A],
            Schema[B],
            Schema[C],
            Schema[D],
            Schema[E],
            Schema[F],
            Schema[G],
            Schema[H],
            Schema[I],
            Schema[R]
          )
        ](
          q"(${untyped(aSchema)}, ${untyped(bSchema)}, ${untyped(cSchema)}, ${untyped(dSchema)}, ${untyped(
            eSchema
          )}, ${untyped(fSchema)}, ${untyped(gSchema)}, ${untyped(hSchema)}, ${untyped(iSchema)}, ${untyped(rSchema)})"
        )
      )

    val sq = Query9[A, B, C, D, E, F, G, H, I, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag),
      tupleTag(c)(eTag),
      tupleTag(c)(fTag),
      tupleTag(c)(gTag),
      tupleTag(c)(hTag),
      tupleTag(c)(iTag)
    )
    typecheck(sq)(
      schemas1,
      schemas2,
      schemas3,
      schemas4,
      schemas5,
      schemas6,
      schemas7,
      schemas8,
      schemas9,
      schemas10
    ).fold(
      err => c.abort(c.enclosingPosition, err),
      _ =>
        c.Expr[Query9[A, B, C, D, E, F, G, H, I, R]](
          q"_root_.com.spotify.scio.sql.Query9($query, $aTag, $bTag, $cTag, $dTag, $eTag, $fTag, $gTag, $hTag, $iTag)"
        )
    )
  }
}

final class SqlSCollection9[
  A: Schema: ClassTag,
  B: Schema: ClassTag,
  C: Schema: ClassTag,
  D: Schema: ClassTag,
  E: Schema: ClassTag,
  F: Schema: ClassTag,
  G: Schema: ClassTag,
  H: Schema: ClassTag,
  I: Schema: ClassTag
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F],
  g: SCollection[G],
  h: SCollection[H],
  i: SCollection[I]
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
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query9(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(q: Query9[A, B, C, D, E, F, G, H, I, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val collG = Sql.setSchema(g)
      val collH = Sql.setSchema(h)
      val collI = Sql.setSchema(i)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .and(q.eTag, collE.internal)
        .and(q.fTag, collF.internal)
        .and(q.gTag, collG.internal)
        .and(q.hTag, collH.internal)
        .and(q.iTag, collI.internal)
        .apply(
          s"${collA.tfName} join ${collB.tfName} join ${collC.tfName} join ${collD.tfName} join ${collE.tfName} join ${collF.tfName} join ${collG.tfName} join ${collH.tfName} join ${collI.tfName}",
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
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    iTag: TupleTag[I],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query9(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, iTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def queryAs[R: Schema: ClassTag](q: Query9[A, B, C, D, E, F, G, H, I, R]): SCollection[R] =
    try {
      query(
        q.query,
        q.aTag,
        q.bTag,
        q.cTag,
        q.dTag,
        q.eTag,
        q.fTag,
        q.gTag,
        q.hTag,
        q.iTag,
        q.udfs: _*
      ).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query9.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
