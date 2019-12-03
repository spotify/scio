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

package com.spotify.scio.bigquery.syntax

import com.google.api.services.bigquery.model.TableReference

/**
 * Enhanced version of [[com.google.api.services.bigquery.model.TableReference TableReference]].
 */
final class TableReferenceOps(private val r: TableReference) extends AnyVal {
  /**
   * Return table specification in the form of "[project_id]:[dataset_id].[table_id]" or
   * "[dataset_id].[table_id]".
   */
  def asTableSpec: String = {
    require(r.getDatasetId != null, "Dataset can't be null")
    require(r.getTableId != null, "Table can't be null")

    if (r.getProjectId != null) {
      s"${r.getProjectId}:${r.getDatasetId}.${r.getTableId}"
    } else {
      s"${r.getDatasetId}.${r.getTableId}"
    }
  }
}

trait TableReferenceSyntax {
  implicit def bigQueryTableRefOps(r: TableReference): TableReferenceOps = new TableReferenceOps(r)
}
