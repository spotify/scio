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

final case class Query2[A, B, R](
  query: String,
  aTag: TupleTag[A],
  bTag: TupleTag[B],
  udfs: List[Udf] = Nil
)

object Query2 {
  def typecheck[A: Schema, B: Schema, R: Schema](
    q: Query2[A, B, R]
  ): Either[String, Query2[A, B, R]] =
    Queries
      .typecheck(
        q.query,
        List(
          (q.aTag.getId, SchemaMaterializer.beamSchema[A]),
          (q.bTag.getId, SchemaMaterializer.beamSchema[B])
        ),
        SchemaMaterializer.beamSchema[R],
        q.udfs
      )
      .right
      .map(_ => q)
}

final class SqlSCollection2[A: Schema: ClassTag, B: Schema: ClassTag](
  a: SCollection[A],
  b: SCollection[B]
) {

  def query(q: String, aTag: TupleTag[A], bTag: TupleTag[B], udfs: Udf*): SCollection[Row] =
    query(Query2(q, aTag, bTag, udfs.toList))

  def query(q: Query2[A, B, Row]): SCollection[Row] =
    a.context.wrap {
      val collA = Sql.setSchema(a)
      val collB = Sql.setSchema(b)
      val sqlTransform = Sql.registerUdf(SqlTransform.query(q.query), q.udfs: _*)

      PCollectionTuple
        .of(q.aTag, collA.internal)
        .and(q.bTag, collB.internal)
        .apply(s"${collA.tfName} join ${collB.tfName}", sqlTransform)

    }

  def queryAs[R: Schema: ClassTag](
    q: String,
    aTag: TupleTag[A],
    bTag: TupleTag[B],
    udfs: Udf*
  ): SCollection[R] =
    queryAs(Query2(q, aTag, bTag, udfs.toList))

  def queryAs[R: Schema: ClassTag](q: Query2[A, B, R]): SCollection[R] =
    try {
      query(q.query, q.aTag, q.bTag, q.udfs: _*).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query2.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }

}
