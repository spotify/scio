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

package com.spotify.scio

import com.google.api.services.bigquery.model.{TableRow => GTableRow}
import com.spotify.scio.bigquery.syntax.{
  Aliases,
  SCollectionSyntax,
  SCollectionTypedSyntax,
  ScioContextSyntax,
  ScioContextTypedSyntax,
  TableReferenceSyntax,
  TableRowSyntax
}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write

/**
 * Main package for BigQuery APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.bigquery._
 * }}}
 *
 * There are two BigQuery dialects,
 * [[https://cloud.google.com/bigquery/docs/reference/legacy-sql legacy]] and
 * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ standard]].
 * APIs that take a BigQuery query string as argument, e.g.
 * [[com.spotify.scio.bigquery.client.BigQuery.query.rows]],
 * [[com.spotify.scio.bigquery.client.BigQuery.query.schema]],
 * [[com.spotify.scio.bigquery.client.BigQuery.getTypedRows]] and
 * [[com.spotify.scio.bigquery.BigQueryType.fromQuery BigQueryType.fromQuery]], automatically
 * detects the query's dialect. To override this, start the query with either `#legacysql` or
 * `#standardsql` comment line.
 */
package object bigquery
    extends ScioContextSyntax
    with ScioContextTypedSyntax
    with SCollectionSyntax
    with SCollectionTypedSyntax
    with TableRowSyntax
    with TableReferenceSyntax
    with Aliases {

  /** Alias for BigQuery `CreateDisposition`. */
  val CREATE_IF_NEEDED = Write.CreateDisposition.CREATE_IF_NEEDED

  /** Alias for BigQuery `CreateDisposition`. */
  val CREATE_NEVER = Write.CreateDisposition.CREATE_NEVER

  /** Alias for BigQuery `WriteDisposition`. */
  val WRITE_APPEND = Write.WriteDisposition.WRITE_APPEND

  /** Alias for BigQuery `WriteDisposition`. */
  val WRITE_EMPTY = Write.WriteDisposition.WRITE_EMPTY

  /** Alias for BigQuery `WriteDisposition`. */
  val WRITE_TRUNCATE = Write.WriteDisposition.WRITE_TRUNCATE

  /** Alias for BigQuery `TableRow`. */
  type TableRow = GTableRow
}
