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

// Example: Neo4J Input and Output (requires a running Neo4J instance with data)
// Usage:

// `sbt "runMain com.spotify.scio.examples.extra.Neo4JExample
// --project=[PROJECT] --runner=DataflowRunner --region=[REGION NAME]
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.neo4j.{
  Neo4jConnectionOptions,
  Neo4jCypher,
  Neo4jReadOptions,
  Neo4jWrite,
  Neo4jWriteOptions
}
import org.neo4j.driver.Record

object Neo4JExample {

  private case class Entity(id: String, property: Option[String])

  private val neo4jConf = Neo4jConnectionOptions("neo4j://neo4j.com:7687", "username", "password")

  private val readCypher = "MATCH (e:Entity) WHERE e.property = \"value\" RETURN e"

  // This can be implemented using https://github.com/neotypes/neotypes
  // e.g. using val entityMapper = neotypes.ResultMapper[Entity] = implicitly
  private def neo4JRecordToEntity(record: Record): Entity = Entity(
    record.get(0).get("id").asString(),
    Option(record.get(0).get("column")).map(_.asString())
  )

  private val writeCypher = "UNWIND $rows AS row MERGE (e:Entity {id:row.id}) " +
    "ON CREATE SET p.id = row.id, p.property = row.property"

  def setter(entity: Entity): Map[String, AnyRef] = Map[String, AnyRef](
    "id" -> entity.id,
    "property" -> entity.property.orNull
  )

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs)

    val entities =
      sc.read(Neo4jCypher(Neo4jReadOptions(neo4jConf, readCypher, neo4JRecordToEntity)))

    val modifiedEntities = entities.map(e => e.copy(property = e.property.map(_ + " modified")))

    modifiedEntities.write(Neo4jWrite(Neo4jWriteOptions(neo4jConf, writeCypher, "rows", setter)))
  }
}
