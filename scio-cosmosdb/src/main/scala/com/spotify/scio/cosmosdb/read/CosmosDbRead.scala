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

package com.spotify.scio.cosmosdb.read

import com.spotify.scio.annotations.experimental
import org.apache.beam.sdk.io.Read
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.{ PBegin, PCollection }
import org.bson.Document
import org.slf4j.LoggerFactory

/** A [[PTransform]] to read data from CosmosDB Core (SQL) API. */
@experimental
private[cosmosdb] case class CosmosDbRead(
  endpoint: String,
  key: String,
  database: String,
  container: String,
  query: String = null
) extends PTransform[PBegin, PCollection[Document]] {
  private val log = LoggerFactory.getLogger(classOf[CosmosDbRead])

  override def expand(input: PBegin): PCollection[Document] = {
    log.debug(s"Read CosmosDB with endpoint: $endpoint and query: $query")
    input.apply(Read.from(new CosmosDbBoundedSource(endpoint, key, database, container, query)))
  }

}
