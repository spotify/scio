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

import java.io.StringReader
import java.util.UUID
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.TableSchema

/** Utility for BigQuery data types. */
object BigQueryUtil {
  private lazy val jsonObjectParser = new JsonObjectParser(GsonFactory.getDefaultInstance)

  /** Parse a schema string. */
  def parseSchema(schemaString: String): TableSchema =
    jsonObjectParser
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  /* Generates job ID */
  def generateJobId(projectId: String): String =
    projectId + "-" + UUID.randomUUID().toString
}
