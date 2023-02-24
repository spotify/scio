/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio.neo4j.syntax

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.neo4j.{Neo4jIO, Neo4jOptions}
import com.spotify.scio.values.SCollection
import magnolify.neo4j.ValueType
import org.apache.beam.sdk.io.{neo4j => beam}

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Neo4J methods. */
final class Neo4jSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  import Neo4jIO._

  /**
   * Execute parallel instances of the provided Cypher query to the specified Neo4j database; one
   * instance of the query will be executed for each element in this [[SCollection]]. Results from
   * each query invocation will be added to the resulting [[SCollection]] as if by a `flatMap`
   * transformation (where the Neo4j-query-execution returns an `Iterable`).
   *
   * This operation parameterizes each query invocation by applying a supplied function to each
   * [[SCollection]] element before executing the Cypher query. The function must produce a [[Map]]
   * of [[String]] to [[AnyRef]], where the keys correspond to named parameters in the provided
   * Cypher query and the corresponding values correspond to the intended value of that parameter
   * for a given query invocation. Named parameters must consist of letters and numbers, prepended
   * with a `$`, as described in the Neo4j
   * [[https://neo4j.com/docs/cypher-manual/current/syntax/parameters Cypher Manual (Syntax / Parameters)]].
   *
   * @see
   *   ''Reading from Neo4j'' in the
   *   [[https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/neo4j/Neo4jIO.html Beam `Neo4jIO` documentation]]
   * @param neo4jConf
   *   options for configuring a Neo4J driver
   * @param cypher
   *   parameterized Cypher query
   * @return
   *   [[SCollection]] containing the union of query results from a parameterized query invocation
   *   for each original [[SCollection]] element
   */
  def neo4jCypher[U](
    neo4jConf: Neo4jOptions,
    cypher: String
  )(implicit
    neo4jInType: ValueType[T],
    neo4jOutType: ValueType[U],
    coder: Coder[U]
  ): SCollection[U] =
    self.applyTransform(
      beam.Neo4jIO
        .readAll[T, U]()
        .withDriverConfiguration(dataSourceConfiguration(neo4jConf.connectionOptions))
        .withSessionConfig(neo4jConf.sessionConfig)
        .withTransactionConfig(neo4jConf.transactionConfig)
        .withCypher(cypher)
        .withParametersFunction(neo4jInType.to(_).asMap())
        .withRowMapper(neo4jOutType.from(_))
        .withCoder(CoderMaterializer.beam(self.context, coder))
    )

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

trait SCollectionSyntax {
  implicit def neo4jSCollectionOps[T](sc: SCollection[T]): Neo4jSCollectionOps[T] =
    new Neo4jSCollectionOps(sc)
}
