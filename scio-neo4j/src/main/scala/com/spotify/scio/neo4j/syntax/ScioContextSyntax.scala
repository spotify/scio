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

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.neo4j.{Neo4jCypher, Neo4jReadOptions}
import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

/** Enhanced version of [[ScioContext]] with Neo4J methods. */
final class Neo4jScioContextOps(private val self: ScioContext) extends AnyVal {

  /** Get an SCollection for a Neo4J cypher query. */
  def neo4jCypher[T: ClassTag: Coder](readOptions: Neo4jReadOptions[T]): SCollection[T] =
    self.read(Neo4jCypher(readOptions))

}
trait ScioContextSyntax {
  implicit def neo4jScioContextOps(sc: ScioContext): Neo4jScioContextOps = new Neo4jScioContextOps(
    sc
  )
}
