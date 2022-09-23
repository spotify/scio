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

object Neo4jIOTest {
  final case class Data(value: String)
}

class Neo4jIOTest extends ScioIOSpec {

  import Neo4jIOTest._

  val options: Neo4jOptions = {
    val connectionOptions = Neo4jConnectionOptions(
      url = "neo4j://neo4j.com:7687",
      username = "john",
      password = "secret"
    )
    Neo4jOptions(connectionOptions)
  }

  "Neo4jIO" should "extract unwindMapName from cypher" in {
    def write(unwindCypher: String): Neo4jIO[Data] =
      Neo4jIO[Data](options, unwindCypher)

    write("WITH SOME STUFF UNWIND $rows AS row REST OF QUERY").unwindMapName shouldBe "rows"
    write("WITH SOME STUFF\nUNWIND $rows AS row\nREST OF QUERY").unwindMapName shouldBe "rows"

    // not unwind
    an[IllegalArgumentException] shouldBe thrownBy {
      val cypher = "CREATE (n:Person {name: 'Andy', title: 'Developer'})"
      write(cypher).unwindMapName
    }

    // unwind value is not a parameter
    an[IllegalArgumentException] shouldBe thrownBy {
      val cypher =
        """WITH [1, 1, 2, 2] AS coll
          |UNWIND coll AS x
          |WITH DISTINCT x
          |RETURN collect(x) AS setOfVals
          |""".stripMargin
      write(cypher).unwindMapName
    }
  }

  it should "support neo4j cypher input" in {
    val input = Seq(Data("a"), Data("b"), Data("c"))
    val cypher = "MATCH (t:This) RETURN t.that as value"
    testJobTestInput(input, cypher)(Neo4jIO(options, _))(_.neo4jCypher(options, _))
  }

  it should "support neo4j unwind cypher output" in {
    val input = Seq(Data("a"), Data("b"), Data("c"))
    val cypher = "UNWIND $rows AS row MERGE (t:This {that: row.value})"
    testJobTestOutput(input, cypher)(Neo4jIO(options, _))(_.saveAsNeo4j(options, _))
  }
}
