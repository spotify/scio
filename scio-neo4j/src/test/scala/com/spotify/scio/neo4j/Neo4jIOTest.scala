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

import com.spotify.scio.testing._

class Neo4jIOTest extends ScioIOSpec {

  val options: Neo4jOptions = {
    val connectionOptions = Neo4jConnectionOptions(
      url = "neo4j://neo4j.com:7687",
      username = "john",
      password = "secret"
    )
    Neo4jOptions(connectionOptions)
  }

  "Neo4jWriteUnwind" should "extract unwindMapName from cypher" in {
    implicit val builder: ParametersBuilder[String] = _ => Map.empty
    def write(unwindCypher: String): Neo4jWriteUnwind[String] =
      Neo4jWriteUnwind[String](options, unwindCypher)

    write("WITH SOME STUFF UNWIND $rows AS row REST OF QUERY").unwindMapName shouldBe "rows"
    write("WITH SOME STUFF\nUNWIND $rows AS row\nREST OF QUERY").unwindMapName shouldBe "rows"

    // not unwind
    an[IllegalArgumentException] shouldBe thrownBy {
      val cypher = "CREATE (n:Person {name: 'Andy', title: 'Developer'})"
      write(cypher)
    }

    // unwind value is not a parameter
    an[IllegalArgumentException] shouldBe thrownBy {
      val cypher =
        """WITH [1, 1, 2, 2] AS coll
          |UNWIND coll AS x
          |WITH DISTINCT x
          |RETURN collect(x) AS setOfVals
          |""".stripMargin
      write(cypher)
    }
  }

  "Neo4jIO" should "support neo4j cypher input" in {
    val input = Seq("a", "b", "c")
    val cypher = "MATCH (t:This) RETURN t.that"
    implicit val mapper: RowMapper[String] = _.get(0).asString()
    testJobTestInput(input, cypher)(Neo4jIO(options, _))(_.neo4jCypher(options, _))
  }

  it should "support neo4j unwind cypher output" in {
    val input = Seq("a", "b", "c")
    val cypher = "UNWIND $rows AS row MERGE (t:This {that:row.that})"
    implicit val builder: ParametersBuilder[String] = s => Map("that" -> s)
    testJobTestOutput(input, cypher)(Neo4jIO(options, _))(_.saveAsNeo4j(options, _))
  }
}
