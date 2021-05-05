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

final case class Query8[A, B, C, D, E, F, G, H, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  cTag: TupleTag[C],
  dTag: TupleTag[D],
  eTag: TupleTag[E],
  fTag: TupleTag[F],
  gTag: TupleTag[G],
  hTag: TupleTag[H],
  udfs: List[Udf] = Nil
)

object Query8 {

  def typecheck[
    A: Schema,
    B: Schema,
    C: Schema,
    D: Schema,
    E: Schema,
    F: Schema,
    G: Schema,
    H: Schema,
    R: Schema
  ](q: Query8[A, B, C, D, E, F, G, H, R]): Either[String, Query8[A, B, C, D, E, F, G, H, R]] =
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
          (q.hTag.getId, SchemaMaterializer.beamSchema[H])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)
}

final class SqlSCollection8[
  A: Schema: ClassTag,
  B: Schema: ClassTag,
  C: Schema: ClassTag,
  D: Schema: ClassTag,
  E: Schema: ClassTag,
  F: Schema: ClassTag,
  G: Schema: ClassTag,
  H: Schema: ClassTag
](
  a: SCollection[A],
  b: SCollection[B],
  c: SCollection[C],
  d: SCollection[D],
  e: SCollection[E],
  f: SCollection[F],
  g: SCollection[G],
  h: SCollection[H]
) {

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
    udfs: Udf*
  ): SCollection[Row] =
    query(Query8(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, udfs.toList))

  def query(q: Query8[A, B, C, D, E, F, G, H, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val collC = Sql.setSchema(c)
      val collD = Sql.setSchema(d)
      val collE = Sql.setSchema(e)
      val collF = Sql.setSchema(f)
      val collG = Sql.setSchema(g)
      val collH = Sql.setSchema(h)
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
        .apply(
          s"${collA.tfName} join ${collB.tfName} join ${collC.tfName} join ${collD.tfName} join ${collE.tfName} join ${collF.tfName} join ${collG.tfName} join ${collH.tfName}",
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
    fTag: TupleTag[F],
    gTag: TupleTag[G],
    hTag: TupleTag[H],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query8(q, aTag, bTag, cTag, dTag, eTag, fTag, gTag, hTag, udfs.toList))

  def queryAs[R: Schema: ClassTag](q: Query8[A, B, C, D, E, F, G, H, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.eTag, q.fTag, q.gTag, q.hTag, q.udfs: _*)
        .to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query8.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
