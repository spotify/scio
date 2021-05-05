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
}

final class SqlSCollection4[
  A: Schema: ClassTag,
  B: Schema: ClassTag,
  C: Schema: ClassTag,
  D: Schema: ClassTag
](a: SCollection[A], b: SCollection[B], c: SCollection[C], d: SCollection[D]) {

  def query(
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    udfs: Udf*
  ): SCollection[Row] =
    query(Query4(q, aTag, bTag, cTag, dTag, udfs.toList))

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

  def queryAs[R: Schema: ClassTag](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    cTag: TupleTag[C],
    dTag: TupleTag[D],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query4(q, aTag, bTag, cTag, dTag, udfs.toList))

  def queryAs[R: Schema: ClassTag](q: Query4[A, B, C, D, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.cTag, q.dTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query4.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
