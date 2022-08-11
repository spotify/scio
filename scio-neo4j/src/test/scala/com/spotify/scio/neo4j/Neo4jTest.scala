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

package com.spotify.scio.neo4j

import com.spotify.scio._
import com.spotify.scio.testing._
import org.neo4j.driver.Record

object Neo4jJob {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.neo4jCypher(getReadOptions(args))
      .map(_ + "N")
      .saveAsNeo4j(getWriteOptions(args))
    sc.run()
    ()
  }

  def getReadOptions(args: Args): Neo4jReadOptions[String] =
    Neo4jReadOptions(
      connectionOptions = getConnectionOptions(args),
      cypher = "MATCH (t:This) RETURN t.that",
      rowMapper = (r: Record) => r.get(0).asString()
    )

  def getWriteOptions(args: Args): Neo4jWriteOptions[String] =
    Neo4jWriteOptions[String](
      connectionOptions = getConnectionOptions(args),
      cypher = "UNWIND $rows AS row MERGE (t:This {that:row.that})",
      unwindMapName = "rows",
      parametersFunction = s => Map[String, AnyRef]("that" -> s)
    )

  def getConnectionOptions(args: Args): Neo4jConnectionOptions =
    Neo4jConnectionOptions(
      url = args("neo4jUrl"),
      username = args("neo4jUsername"),
      password = args("neo4jPassword")
    )
}

class Neo4jTest extends PipelineSpec {
  def testNeo4j(xs: String*): Unit = {
    val cmdlineArgs = Seq(
      "--neo4jUrl=neo4j://neo4j.com:7687",
      "--neo4jUsername=john",
      "--neo4jPassword=secret"
    )
    val (_, args) = ContextAndArgs(cmdlineArgs.toArray)
    val readOpts = Neo4jJob.getReadOptions(args)
    val writeOpts = Neo4jJob.getWriteOptions(args)

    JobTest[Neo4jJob.type]
      .args(cmdlineArgs: _*)
      .input(Neo4jIO[String](readOpts), Seq("a", "b", "c"))
      .output(Neo4jIO[String](writeOpts))(coll => coll should containInAnyOrder(xs))
      .run()
  }

  it should "pass correct Neo4J" in {
    testNeo4j("aN", "bN", "cN")
  }

  it should "fail incorrect Neo4J" in {
    an[AssertionError] should be thrownBy { testNeo4j("aN", "bN") }
    an[AssertionError] should be thrownBy { testNeo4j("aN", "bN", "cN", "dN") }
  }
}
