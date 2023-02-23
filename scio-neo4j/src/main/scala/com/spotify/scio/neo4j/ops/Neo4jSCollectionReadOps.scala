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

import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.neo4j.Neo4jOptions
import com.spotify.scio.neo4j.ops.Neo4jCommon.dataSourceConfiguration
import com.spotify.scio.values.SCollection
import magnolify.neo4j.ValueType
import org.apache.beam.sdk.io.{neo4j => beam}
import org.apache.beam.sdk.transforms.SerializableFunction

import java.util.{Map => JMap}
import scala.jdk.CollectionConverters._

/**
 * Operations for transforming an existing [[SCollection]] via a Neo4j query (read)
 * @param self
 *   existing [[SCollection]] (prior to Neo4j read)
 * @tparam X
 *   type parameter for existing [[SCollection]]
 */
class Neo4jSCollectionReadOps[X](self: SCollection[X]) {

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
   *   [[Neo4jOptions]] indicating the Neo4j instance on which to run the query
   * @param cypher
   *   [[String]] parameterized Cypher query
   * @param elementToParamFunction
   *   function (`X => Map[String, AnyRef]`) which converts an element of the [[SCollection]] to a
   *   [[Map]] of parameter values for the given Cypher query
   * @param neo4jType
   *   (implicit) [[ValueType]] converting Neo4j results to the expected [[SCollection]] output type
   * @param coder
   *   (implicit) [[Coder]] for serialization of the result [[SCollection]] type
   * @tparam Y
   *   result [[SCollection]] type
   * @return
   *   [[SCollection]] containing the union of query results from a parameterized query invocation
   *   for each original [[SCollection]] element
   */
  def neo4jCypherWithParams[Y](
    neo4jConf: Neo4jOptions,
    cypher: String,
    elementToParamFunction: X => Map[String, AnyRef]
  )(implicit
    neo4jType: ValueType[Y],
    coder: Coder[Y]
  ): SCollection[Y] = {
    Neo4jSCollectionReadOps.neo4jCypherWithParamsImpl(
      self,
      neo4jConf,
      cypher,
      elementToParamFunction
    )
  }

}

/**
 * Implementations for operations for transforming an existing [[SCollection]] via a Neo4j query;
 * typically used via implicitly-defined [[SCollection]] syntax.
 * @see
 *   [[com.spotify.scio.neo4j.syntax.SCollectionSyntax]]
 */
object Neo4jSCollectionReadOps {

  import Neo4jCommonImplicits._

  /**
   * Convert a provided Scala function to a Beam [[SerializableFunction]] for use in
   * [[https://beam.apache.org/releases/javadoc/current/org/apache/beam/sdk/io/neo4j/Neo4jIO.ReadAll.html#withParametersFunction-org.apache.beam.sdk.transforms.SerializableFunction- `Neo4jIO.ReadAll`]]
   * @param elementToParamFunction
   *   `X => Map[String, AnyRef]` Scala function defining how elements of an existing
   *   [[SCollection]] (`X`) should map to parameters ([[String]] keys in the output [[Map]]) in a
   *   Cypher query
   * @tparam X
   *   type parameter for input [[SCollection]]
   * @return
   *   [[SerializableFunction]] equivalent of input Scala function
   */
  private def beamParamFunction[X](
    elementToParamFunction: X => Map[String, AnyRef]
  ): SerializableFunction[X, JMap[String, AnyRef]] = {
    new SerializableFunction[X, JMap[String, AnyRef]] {
      override def apply(input: X): JMap[String, AnyRef] = {
        val queryParamStringToArgValue: Map[String, AnyRef] = elementToParamFunction.apply(input)
        queryParamStringToArgValue.asJava
      }
    }
  }

  /** @see [[com.spotify.scio.neo4j.ops.Neo4jSCollectionReadOps.neo4jCypherWithParams]] */
  def neo4jCypherWithParamsImpl[X, Y](
    sColl: SCollection[X],
    neo4jConf: Neo4jOptions,
    cypher: String,
    elementToParamFunction: X => Map[String, AnyRef]
  )(implicit
    neo4jType: ValueType[Y],
    coder: Coder[Y]
  ): SCollection[Y] = {
    sColl
      .applyTransform(
        beam.Neo4jIO
          .readAll[X, Y]()
          .withDriverConfiguration(dataSourceConfiguration(neo4jConf.connectionOptions))
          .withSessionConfig(neo4jConf.sessionConfig)
          .withTransactionConfig(neo4jConf.transactionConfig)
          .withCypher(cypher)
          .withParametersFunction(beamParamFunction(elementToParamFunction))
          .withRowMapper(neo4jType.from(_))
          .withCoder(CoderMaterializer.beam(sColl.context, coder))
      )
  }

}
