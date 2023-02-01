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

import org.apache.beam.sdk.annotations.Experimental
import org.apache.beam.sdk.annotations.Experimental.Kind
import org.apache.beam.sdk.coders.{Coder, SerializableCoder}
import org.apache.beam.sdk.io.BoundedSource
import org.apache.beam.sdk.options.PipelineOptions
import org.bson.Document

import java.util
import java.util.Collections

/**
 * A CosmosDB Core (SQL) API [[BoundedSource]] reading [[Document]] from a given instance.
 */
@Experimental(Kind.SOURCE_SINK)
private[read] class CosmosDbBoundedSource(private[read] val readCosmos: CosmosDbRead)
    extends BoundedSource[Document] {

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
