package com.spotify.scio.neo4j

import com.dimafeng.testcontainers.{ForAllTestContainer, Neo4jContainer}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase}
import org.scalatest.concurrent.Eventually

import java.time.Year

import scala.jdk.CollectionConverters._

object Neo4jIOIT {
  final case class Person(name: String)
  final case class Movie(title: String, year: Year)
  final case class Role(person: Person, movie: Movie, role: String)
}

class Neo4jIOIT extends PipelineSpec with Eventually with ForAllTestContainer {

  import Neo4jIOIT._

  override val container: Neo4jContainer = Neo4jContainer()

  lazy val client: Driver = GraphDatabase.driver(
    container.boltUrl,
    AuthTokens.basic(container.username, container.password)
  )

  // creating data from
  // https://neo4j.com/docs/getting-started/current/cypher-intro/load-csv/#_the_graph_model
  // (m)-[:ORIGIN]->(c) relation is added in the write test
  "Neo4jIO" should "read cypher query from the graph database" in {
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
            | (wallStreet:Movie {name: 'Wall Street', year: 1987}),
            | (americanPresident:Movie {name: 'American President', year: 1995}),
            | (theShawshankRedemption:Movie {name: 'The Shawshank Redemption', year: 1994}),
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

    val options = PipelineOptionsFactory.create()
    options.setRunner(classOf[DirectRunner])

    val martin = Person("Martin Sheen")
    val expectedRoles = Seq(
      Role(martin, Movie("Wall Street", Year.of(1987)), "Carl Fox"),
      Role(martin, Movie("American President", Year.of(1995)), "A.J. MacInerney")
    )

    val neo4jOptions = Neo4jReadOptions(
      Neo4jConnectionOptions(container.boltUrl, container.username, container.password),
      s"MATCH (p)-[r: ACTED_IN]->(m) WHERE p.name='${martin.name}' RETURN p, r, m",
      row => {
        val p = Person(row.get("p").get("name").asString())
        val m =
          Movie(row.get("m").get("name").asString(), Year.of(row.get("m").get("year").asInt()))
        Role(p, m, row.get("r").get("role").asString())
      }
    )

    runWithRealContext(options) { sc =>
      sc.neo4jCypher(neo4jOptions) should containInAnyOrder(expectedRoles)
    }
  }

  it should "write to the graph database" in {
    val options = PipelineOptionsFactory.create()
    options.setRunner(classOf[DirectRunner])

    val movieNames = Seq("Wall Street", "American President", "The Shawshank Redemption")

    val neo4jOptions = Neo4jWriteOptions[(String, String)](
      Neo4jConnectionOptions(container.boltUrl, container.username, container.password),
      """UNWIND $origin AS origin
        |MATCH
        |  (m:Movie {name: origin.movie}),
        |  (c:Country {name: origin.country})
        |CREATE (m)-[:ORIGIN]->(c)
        |""".stripMargin,
      "origin",
      { case (movie, country) => Map("movie" -> movie, "country" -> country) }
    )

    runWithRealContext(options) { sc =>
      sc.parallelize(movieNames.map(_ -> "USA")).saveAsNeo4j(neo4jOptions)
    }

    val session = client.session()
    try {
      val records = session.readTransaction { tx =>
        tx.run(s"MATCH (m)-[:ORIGIN]->(c) WHERE c.name='USA' RETURN m.name as movie").list()
      }
      records.asScala.foreach(println)
      records.asScala.map(_.get("movie").asString()) should contain theSameElementsAs movieNames
    } finally session.close()
  }
}
