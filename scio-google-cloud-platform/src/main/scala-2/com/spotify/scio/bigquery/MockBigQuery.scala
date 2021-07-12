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

package com.spotify.scio.bigquery

import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait MockTypedBigQuery {
  self: MockBigQuery =>

  /**
   * Get result of a live query against BigQuery service, substituting mocked tables with test
   * data.
   */
  def typedQueryResult[T <: HasAnnotation: ClassTag: TypeTag](
    sqlQuery: String,
    flattenResults: Boolean = false
  ): Seq[T] = {
    val bqt = BigQueryType[T]
    queryResult(sqlQuery, flattenResults).map(bqt.fromTableRow)
  }
}

trait MockTypedTable {
  self: MockTable =>

  /** Populate the table with mock data. */
  def withTypedData[T <: HasAnnotation: ClassTag: TypeTag](rows: Seq[T]): Unit = {
    val bqt = BigQueryType[T]
    withData(rows.map(bqt.toTableRow))
  }
}
