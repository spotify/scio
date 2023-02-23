package com.spotify.scio.neo4j

import com.dimafeng.testcontainers.{ForAllTestContainer, Neo4jContainer}
import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.neo4j.driver.{AuthTokens, Driver, GraphDatabase, Record}
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

    val options = PipelineOptionsFactory.create()
    options.setRunner(classOf[DirectRunner])

    val martin = Person("Martin Sheen")
    val morgan = Person("Morgan Freeman")
    val michael = Person("Michael Douglas")

    val americanPresident = Movie("American President", 1995)

    val queryMovieYears = Seq(1994, 0, 1995)
    val expectedRolesMartin = Seq(
      Role(martin, Movie("Wall Street", 1987), "Carl Fox"),
      Role(martin, Movie("American President", 1995), "A.J. MacInerney")
    )
    val expectedRolesMovieYears = Seq(
      Role(martin, americanPresident, "A.J. MacInerney"),
      Role(michael, americanPresident, "President Andrew Shepherd"),
      Role(morgan, Movie("The Shawshank Redemption", 1994), "Ellis Boyd 'Red' Redding")
    )

    val queryRoles =
      s"""MATCH (p)-[r: ACTED_IN]->(m)
         |WHERE p.name='${martin.name}'
         |RETURN p as person, m as movie, r.role as role
         |""".stripMargin

    val queryMovieYear =
      s"""MATCH (p)-[r: ACTED_IN]->(m)
         |WHERE m.year = $$movieYear
         |RETURN p as person, m as movie, r.role as role
         |""".stripMargin

    val neo4jOptions = Neo4jOptions(
      Neo4jConnectionOptions(container.boltUrl, container.username, container.password)
    )

    implicit val rowMapper = (record: Record) => {
      val p = Person(record.get("p").get("name").asString())
      val m = Movie(record.get("m").get("name").asString(), record.get("m").get("year").asInt())
      Role(p, m, record.get("r").get("role").asString())
    }

    runWithRealContext(options) { sc =>
      val resultQueryRoles = sc.neo4jCypher[Role](neo4jOptions, queryRoles)
      resultQueryRoles should containInAnyOrder(expectedRolesMartin)

      val resultQueryMovieYear = sc
        .parallelize(queryMovieYears)
        .neo4jCypherWithParams[Role](
          neo4jOptions,
          queryMovieYear,
          (my: Int) => Map("movieYear" -> java.lang.Integer.valueOf(my))
        )
      resultQueryMovieYear should containInAnyOrder(expectedRolesMovieYears)
    }
  }

  it should "write to the graph database" in {
    val options = PipelineOptionsFactory.create()
    options.setRunner(classOf[DirectRunner])

    val movieOrigins = Seq(
      Origin("Wall Street", "USA"),
      Origin("American President", "USA"),
      Origin("The Shawshank Redemption", "USA")
    )

    val neo4jOptions = Neo4jOptions(
      Neo4jConnectionOptions(container.boltUrl, container.username, container.password)
    )

    val insertOrigins = """UNWIND $origin AS origin
                          |MATCH
                          |  (m:Movie {title: origin.movie}),
                          |  (c:Country {name: origin.country})
                          |CREATE (m)-[:ORIGIN]->(c)
                          |""".stripMargin
    runWithRealContext(options) { sc =>
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
