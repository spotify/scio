/*
 * Copyright 2016 Spotify AB.
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

import com.spotify.scio.values._
import com.spotify.scio.coders._
import com.spotify.scio.schemas.{Record, Schema, SchemaMaterializer}
import org.apache.beam.sdk.values._
import org.apache.beam.sdk.extensions.sql.SqlTransform
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlEnv
import org.apache.beam.sdk.extensions.sql.impl.schema.BeamPCollectionTable
import org.apache.beam.sdk.extensions.sql.impl.utils.CalciteUtils
import com.google.common.collect.ImmutableMap

import scala.language.experimental.macros

// TODO: could be a PTransform
sealed trait Query[I, O] {
  val query: String
  def run(c: SCollection[I]): SCollection[O]
}

object Query {
  import shapeless.tag._

  type TypedRow = Row @@ Schema[Row]

  val PCOLLECTION_NAME = "PCOLLECTION"

  def row(q: String): Query[TypedRow, TypedRow] = ???
  def trow(q: String): Query[TypedRow, TypedRow] = ???

  def row[I: Record](q: String): Query[I, TypedRow] =
    new Query[I, TypedRow] {
      val query = q
      def run(c: SCollection[I]) = {
        val scoll = c.setSchema(Record[I])
        val sqlEnv = BeamSqlEnv.readOnly(
          PCOLLECTION_NAME,
          ImmutableMap.of(PCOLLECTION_NAME, new BeamPCollectionTable(scoll.internal)))
        // Will it support UDF (see SqlTransform.expand) ?
        val q = sqlEnv.parseQuery(query)
        val schema = CalciteUtils.toSchema(q.getRowType)
        scoll
          .applyTransform[Row](SqlTransform.query(query))(Coder.row(schema))
          .asInstanceOf[SCollection[TypedRow]]
      }
    }

  def of[I: Record, O: Schema](q: String): Query[I, O] =
    new Query[I, O] {
      val query = q
      def run(s: SCollection[I]): SCollection[O] = {
        import org.apache.beam.sdk.schemas.SchemaCoder
        val (schema, to, from) = SchemaMaterializer.materialize(s.context, Schema[O])
        val coll = Query.row[I](query).run(s)
        coll.map[O](r => from(r))(Coder.beam(SchemaCoder.of(schema, to, from)))
      }
    }

  def tsql[I: Record, O: Schema](query: String): Query[I, O] =
    macro QueryMacros.typecheck[I, O]
}
