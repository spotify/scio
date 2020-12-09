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

import com.spotify.scio.schemas._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.ParseException
import org.apache.beam.sdk.values._

import scala.reflect.ClassTag

final case class Query1[A, B](
  query: String,
  tag: TupleTag[A] = Sql.defaultTag[A],
  udfs: List[Udf] = Nil
)

object Query1 {
  /**
   * Typecheck [[Query1]] q against the provided schemas.
   * If the query correctly typechecks, it's simply return as a [[Right]].
   * If it fails, a error message is returned in a [[Left]].
   */
  def typecheck[A: Schema, B: Schema](q: Query1[A, B]): Either[String, Query1[A, B]] =
    Queries
      .typecheck(
        q.query,
        List((q.tag.getId, SchemaMaterializer.beamSchema[A])),
        SchemaMaterializer.beamSchema[B],
        q.udfs
      )
      .right
      .map(_ => q)
}

final class SqlSCollection1[A: Schema: ClassTag](sc: SCollection[A]) {
  def query(q: String, udfs: Udf*): SCollection[Row] =
    query(Query1[A, Row](q, Sql.defaultTag, udfs = udfs.toList))

  def query(q: Query1[A, Row]): SCollection[Row] =
    sc.context.wrap {
      val scWithSchema = Sql.setSchema(sc)
      val transform =
        SqlTransform
          .query(q.query)
          .withTableProvider(Sql.BeamProviderName, Sql.tableProvider(q.tag, scWithSchema))
      val sqlTransform = Sql.registerUdf(transform, q.udfs: _*)
      scWithSchema.applyInternal(sqlTransform)
    }

  def queryAs[R: Schema: ClassTag](q: String, udfs: Udf*): SCollection[R] =
    queryAs(Query1[A, R](q, Sql.defaultTag, udfs = udfs.toList))

  def queryAs[R: Schema: ClassTag](q: Query1[A, R]): SCollection[R] =
    try {
      query(Query1[A, Row](q.query, q.tag, q.udfs)).to(To.unchecked((_, i) => i))
    } catch {
      case e: ParseException =>
        Query1.typecheck(q).fold(err => throw new RuntimeException(err, e), _ => throw e)
    }
}
