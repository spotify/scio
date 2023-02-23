/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.neo4j.ops

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.neo4j.Neo4jIO.WriteParam
import com.spotify.scio.neo4j.{Neo4jIO, Neo4jOptions}
import com.spotify.scio.values.SCollection
import magnolify.neo4j.ValueType

/**
 * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Neo4J write methods.
 */
class Neo4jSCollectionWriteOps[T](private val self: SCollection[T]) extends AnyVal {

  /**
   * Save this SCollection as a Neo4J database.
   *
   * @param neo4jOptions
   *   options for configuring a Neo4J driver
   * @param unwindCypher
   *   Neo4J cypher query representing an
   *   [[https://neo4j.com/docs/cypher-manual/current/clauses/unwind/#unwind-creating-nodes-from-a-list-parameter UNWIND parameter]]
   *   cypher statement
   * @param batchSize
   *   batch size when executing the unwind cypher query. Default batch size of 5000
   */
  def saveAsNeo4j(
    neo4jOptions: Neo4jOptions,
    unwindCypher: String,
    batchSize: Long = WriteParam.BeamDefaultBatchSize
  )(implicit neo4jType: ValueType[T], coder: Coder[T]): ClosedTap[Nothing] =
    self.write(Neo4jIO[T](neo4jOptions, unwindCypher))(WriteParam(batchSize))

}
