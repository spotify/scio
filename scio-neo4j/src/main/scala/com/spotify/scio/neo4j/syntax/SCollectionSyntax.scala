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

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.neo4j.{Neo4jWrite, Neo4jWriteOptions}
import com.spotify.scio.values.SCollection

/** Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with Neo4J methods. */
final class Neo4jSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /** Save this SCollection as a Neo4J database. */
  def saveAsNeo4j(writeOptions: Neo4jWriteOptions[T]): ClosedTap[Nothing] =
    self.write(Neo4jWrite(writeOptions))
}

trait SCollectionSyntax {
  implicit def neo4jSCollectionOps[T](sc: SCollection[T]): Neo4jSCollectionOps[T] =
    new Neo4jSCollectionOps(sc)
}
