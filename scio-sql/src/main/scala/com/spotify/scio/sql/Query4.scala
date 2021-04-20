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

final case class Query4[A, B, C, D, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  udfs: List[Udf] = Nil
)

object Query4 {
  import scala.reflect.macros.blackbox
  import QueryMacros._

  def typecheck[A: Schema, B: Schema, C: Schema, D: Schema, R: Schema](
    q: Query4[A, B, C, D, R]
  ): Either[String, Query4[A, B, C, D, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B]),
          (q.cTag.getId, SchemaMaterializer.beamSchema[C]),
          (q.dTag.getId, SchemaMaterializer.beamSchema[D])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)

  def typed[A: Schema, B: Schema, C: Schema, D: Schema, R: Schema](
    query: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D]
  ): Query4[A, B, C, D, R] =
    macro typed4Impl[A, B, C, D, R]

  def typed4Impl[
    A: c.WeakTypeTag,
    B: c.WeakTypeTag,
    C: c.WeakTypeTag,
    D: c.WeakTypeTag,
    R: c.WeakTypeTag
  ](c: blackbox.Context)(
    query: c.Expr[String],
    aTag: c.Expr[TupleTag[A]],
    bTag: c.Expr[TupleTag[B]],
    cTag: c.Expr[TupleTag[C]],
    dTag: c.Expr[TupleTag[D]]
  )(
    aSchema: c.Expr[Schema[A]],
    bSchema: c.Expr[Schema[B]],
    cSchema: c.Expr[Schema[C]],
    dSchema: c.Expr[Schema[D]],
    rSchema: c.Expr[Schema[R]]
  ): c.Expr[Query4[A, B, C, D, R]] = {
    val h = new { val ctx: c.type = c } with SchemaMacroHelpers
    import h._
    import c.universe._

    assertConcrete[A](c)
    assertConcrete[B](c)
    assertConcrete[C](c)
    assertConcrete[D](c)
    assertConcrete[R](c)

    val (schemas1, schemas2, schemas3, schemas4, schemas5) =
      c.eval(
        c.Expr[(Schema[A], Schema[B], Schema[C], Schema[D], Schema[R])](
          q"(${untyped(aSchema)}, ${untyped(bSchema)}, ${untyped(cSchema)}, ${untyped(dSchema)}, ${untyped(rSchema)})"
        )
      )

    val sq = Query4[A, B, C, D, R](
      cons(c)(query),
      tupleTag(c)(aTag),
      tupleTag(c)(bTag),
      tupleTag(c)(cTag),
      tupleTag(c)(dTag)
    )
    typecheck(sq)(schemas1, schemas2, schemas3, schemas4, schemas5)
      .fold(
        err => c.abort(c.enclosingPosition, err),
        _ =>
          c.Expr[Query4[A, B, C, D, R]](
            q"_root_.com.spotify.scio.sql.Query4($query, $aTag, $bTag, $cTag, $dTag)"
          )
      )
  }
}

final class SqlSCollection4[
  A: Schema: ClassTag,
  B: Schema: ClassTag,
  C: Schema: ClassTag,
  D: Schema: ClassTag
](a: SCollection[A], b: SCollection[B], c: SCollection[C], d: SCollection[D]) {

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query4(q, aTag, bTag, cTag, dTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def query(q: Query4[A, B, C, D, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .and(q.cTag, collC.internal)
        .and(q.dTag, collD.internal)
        .apply(
          s"${collA.tfName} join ${collB.tfName} join ${collC.tfName} join ${collD.tfName}",
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
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query4(q, aTag, bTag, cTag, dTag, udfs.toList))

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  def queryAs[R: Schema: ClassTag](q: Query4[A, B, C, D, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query4.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
