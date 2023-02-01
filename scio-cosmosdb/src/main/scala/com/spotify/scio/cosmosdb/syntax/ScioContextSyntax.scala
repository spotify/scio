/*
 * Copyright 2023 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.cosmosdb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.cosmosdb.ReadCosmosDdIO
import com.spotify.scio.values.SCollection
import org.bson.Document

trait ScioContextSyntax {
  implicit def cosmosdbScioContextOps(sc: ScioContext): CosmosDbScioContextOps =
    new CosmosDbScioContextOps(sc)
}

final class CosmosDbScioContextOps(private val sc: ScioContext) extends AnyVal {

  /**
   * Read data from CosmosDB CORE (SQL) API
   *
   * Example:
   *
   * url:
   * AccountEndpoint=https://[cosmosdbname].documents.azure.com:443/;AccountKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyy==;
   *
   * @param endpoint
   *   The endpoint, example: AccountEndpoint=https://[cosmosdbname].documents.azure.com:443
   * @param key
   *   The key, example: ;AccountKey=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxyyyyyyyyyyyyyyyyyy==;
   * @param database
   *   The name of the database
   * @param container
   *   The name of the container
   * @param query
   *   The query for cosmosdb, example: "SELECT * FROM c"
   * @return
   */
  def readCosmosDbCoreApi(
    endpoint: String = null,
    key: String = null,
    database: String = null,
    container: String = null,
    query: String = null
  ): SCollection[Document] =
    sc.read(ReadCosmosDdIO(endpoint, key, database, container, query))
}
