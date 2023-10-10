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

package com.spotify.scio.neo4j

import com.dimafeng.testcontainers.{ForAllTestContainer, Neo4jContainer}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.{PipelineOptions, PipelineOptionsFactory}
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}
import org.scalatest.concurrent.Eventually
import org.testcontainers.utility.DockerImageName

import scala.jdk.CollectionConverters._

object Neo4jIOIT {

  val ImageName: DockerImageName = {
    val tag = classOf[Driver].getPackage.getImplementationVersion.take("M.m.p".length)
    DockerImageName.parse("neo4j").withTag(tag)
  }

  final case class Person(name: String)
  final case class Movie(title: String, year: Int)
  final case class Role(person: Person, movie: Movie, role: String)
  final case class Origin(movie: String, country: String)

  final case class MovieParam(year: Int)

}

class Neo4jIOIT extends PipelineSpec with Eventually with ForAllTestContainer {

  import Neo4jIOIT._

  override val container: Neo4jContainer = Neo4jContainer(neo4jImageVersion = ImageName)

  lazy val client: Driver = GraphDatabase.driver(
    container.boltUrl,
    AuthTokens.basic(container.username, container.password)
  )

  // creating data from
  // https://neo4j.com/docs/getting-started/current/cypher-intro/load-csv/#_the_graph_model
  // (m)-[:ORIGIN]->(c) relation is added in the write test
  override def afterStart(): Unit = {
    super.afterStart()
    val session = client.session()
    try {
      session.writeTransaction { tx =>
        tx.run(
          """CREATE
            | (usa:Country {name: 'USA'}),
            | (charlie:Person {name: 'Charlie Sheen'}),
            | (oliver:Person {name: 'Oliver Stone'}),
            | (michael:Person {name: 'Michael Douglas'}),
            | (martin:Person {name: 'Martin Sheen'}),
            | (morgan:Person {name: 'Morgan Freeman'}),
            | (wallStreet:Movie {title: 'Wall Street', year: 1987}),
            | (americanPresident:Movie {title: 'American President', year: 1995}),
            | (theShawshankRedemption:Movie {title: 'The Shawshank Redemption', year: 1994}),
            | (charlie)-[:ACTED_IN {role: 'Bud Fox'}]->(wallStreet),
            | (martin)-[:ACTED_IN {role: 'Carl Fox'}]->(wallStreet),
            | (michael)-[:ACTED_IN {role: 'Gordon Gekko'}]->(wallStreet),
            | (martin)-[:ACTED_IN {role: 'A.J. MacInerney'}]->(americanPresident),
            | (michael)-[:ACTED_IN {role: 'President Andrew Shepherd'}]->(americanPresident),
            | (morgan)-[:ACTED_IN {role: 'Ellis Boyd \'Red\' Redding'}]->(theShawshankRedemption)
            |""".stripMargin
        ).consume()
        ()
      }
    } finally session.close()
  }

  val martin: Person = Person("Martin Sheen")
  val morgan: Person = Person("Morgan Freeman")
  val michael: Person = Person("Michael Douglas")

  val americanPresident: Movie = Movie("American President", 1995)

  lazy val neo4jOptions: Neo4jOptions = Neo4jOptions(
    Neo4jConnectionOptions(container.boltUrl, container.username, container.password)
  )

  "Neo4jIO" should "read cypher query from the graph database" in {
    val queryRoles =
      s"""MATCH (p)-[r: ACTED_IN]->(m)
         |WHERE p.name='${martin.name}'
         |RETURN p as person, m as movie, r.role as role
         |""".stripMargin

    val expectedRoles = Seq(
      Role(martin, Movie("Wall Street", 1987), "Carl Fox"),
      Role(martin, Movie("American President", 1995), "A.J. MacInerney")
    )

    runWithRealContext() { sc =>
      val resultQueryRoles = sc.neo4jCypher[Role](neo4jOptions, queryRoles)
      resultQueryRoles should containInAnyOrder(expectedRoles)
    }
  }

  it should "read cypher query from the graph database with parameter" in {
    val queryParams = Seq(
      MovieParam(1994),
      MovieParam(0),
      MovieParam(1995)
    )

    val queryRoles =
      """MATCH (p)-[r: ACTED_IN]->(m)
        |WHERE m.year = $year
        |RETURN p as person, m as movie, r.role as role
        |""".stripMargin

    val expectedRoles = Seq(
      Role(martin, americanPresident, "A.J. MacInerney"),
      Role(michael, americanPresident, "President Andrew Shepherd"),
      Role(morgan, Movie("The Shawshank Redemption", 1994), "Ellis Boyd 'Red' Redding")
    )

    runWithRealContext() { sc =>
      val resultQueryMovieYear = sc
        .parallelize(queryParams)
        .neo4jCypher[Role](neo4jOptions, queryRoles)

      resultQueryMovieYear should containInAnyOrder(expectedRoles)
    }
  }

  it should "write to the graph database" in {
    val movieOrigins = Seq(
      Origin("Wall Street", "USA"),
      Origin("American President", "USA"),
      Origin("The Shawshank Redemption", "USA")
    )

    val insertOrigins = """UNWIND $origin AS origin
                          |MATCH
                          |  (m:Movie {title: origin.movie}),
                          |  (c:Country {name: origin.country})
                          |CREATE (m)-[:ORIGIN]->(c)
                          |""".stripMargin
    runWithRealContext() { sc =>
      sc
        .parallelize(movieOrigins)
        .saveAsNeo4j(neo4jOptions, insertOrigins)
    }

    val session = client.session()
    try {
      val records = session.readTransaction { tx =>
        tx.run(s"MATCH (m)-[:ORIGIN]->(c) WHERE c.name='USA' RETURN m.title as movie").list()
      }
      val usaMovies = records.asScala.map(_.get("movie").asString())
      usaMovies should contain theSameElementsAs movieOrigins.map(_.movie)
    } finally session.close()
  }
}
