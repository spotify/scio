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
import org.apache.beam.sdk.coders.{Coder, SerializableCoder}
import org.apache.beam.sdk.io.BoundedSource
import org.apache.beam.sdk.options.PipelineOptions
import org.bson.Document

import java.util
import java.util.Collections

/** A CosmosDB Core (SQL) API [[BoundedSource]] reading [[Document]] from a given instance. */
@experimental
private[read] class CosmosDbBoundedSource(
  val endpoint: String,
  val key: String,
  val database: String,
  val container: String,
  val query: String
) extends BoundedSource[Document] {

  require(endpoint != null, "CosmosDB endpoint is required")
  require(key != null, "CosmosDB key is required")
  require(database != null, "CosmosDB database is required")
  require(container != null, "CosmosDB container is required")
  require(query != null, "CosmosDB query is required")

  /**
   * @inheritDoc
   *   TODO: You have to find a better way, maybe by partition key
   */
  override def split(
    desiredBundleSizeBytes: Long,
    options: PipelineOptions
  ): util.List[CosmosDbBoundedSource] =
    Collections.singletonList(this)

  /**
   * @inheritDoc
   *   The Cosmos DB Coro (SQL) API not support this metrics by the querys
   */
  override def getEstimatedSizeBytes(options: PipelineOptions) = 0L

  override def getOutputCoder: Coder[Document] = SerializableCoder.of(classOf[Document])

  override def createReader(options: PipelineOptions) =
    new CosmosDbBoundedReader(this)
}
