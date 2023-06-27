# Neo4J

Scio provides support [Neo4J](https://neo4j.com/) in the `scio-neo4j` artifact.

Scio uses [magnolify's](https://github.com/spotify/magnolify) `magnolify-neo4j` to convert to and from Neo4J types. 

## Static query

@scaladoc[neo4jCypher](com.spotify.scio.neo4j.syntax.Neo4jScioContextOps#neo4jCypher[T](neo4jOptions:com.spotify.scio.neo4j.Neo4jOptions,cypher:String)(implicitevidence$1:magnolify.neo4j.ValueType[T],implicitevidence$2:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]) returns an `SCollection` of results for a Neo4J cypher query, mapped to a specified case class type.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.neo4j._

case class Entity(id: String, property: Option[String])

val sc: ScioContext = ???
val opts = Neo4jOptions(Neo4jConnectionOptions("neo4j://neo4j.com:7687", "username", "password"))
val query =
  """MATCH (e:Entity)
    |WHERE e.property = 'value'
    |RETURN e""".stripMargin
val entities: SCollection[Entity] = sc
  .neo4jCypher[Entity](opts, query)
```

## Parameterized query

@scaladoc[neo4jCypher](com.spotify.scio.neo4j.syntax.Neo4jSCollectionOps#neo4jCypher[U](neo4jConf:com.spotify.scio.neo4j.Neo4jOptions,cypher:String)(implicitneo4jInType:magnolify.neo4j.ValueType[T],implicitneo4jOutType:magnolify.neo4j.ValueType[U],implicitcoder:com.spotify.scio.coders.Coder[U]):com.spotify.scio.values.SCollection[U]) can also construct queries from parameters in an existing `SCollection`:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.neo4j._

case class MovieParam(year: Int)
case class Person(name: String)
case class Movie(title: String, year: Int)
case class Role(person: Person, movie: Movie, role: String)

val sc: ScioContext = ???
val input: SCollection[MovieParam] = sc.parallelize(
  List(
    MovieParam(1994),
    MovieParam(0),
    MovieParam(1995)
  )
)

val opts = Neo4jOptions(Neo4jConnectionOptions("neo4j://neo4j.com:7687", "username", "password"))

val queryRoles =
  """MATCH (p)-[r: ACTED_IN]->(m)
    |WHERE m.year = $year
    |RETURN p as person, m as movie, r.role as role
    |""".stripMargin

input.neo4jCypher[Role](opts, queryRoles)
```

## Writes

Instances can be written via @scaladoc[saveAsNeo4j](com.spotify.scio.neo4j.syntax.Neo4jSCollectionOps#saveAsNeo4j(neo4jOptions:com.spotify.scio.neo4j.Neo4jOptions,unwindCypher:String,batchSize:Long)(implicitneo4jType:magnolify.neo4j.ValueType[T],implicitcoder:com.spotify.scio.coders.Coder[T]):com.spotify.scio.io.ClosedTap[Nothing]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.neo4j._

case class Entity(id: String, property: Option[String])

val sc: ScioContext = ???
val input: SCollection[Entity] = ???

val opts = Neo4jOptions(Neo4jConnectionOptions("neo4j://neo4j.com:7687", "username", "password"))
val unwindCypher =
  """UNWIND $rows AS row
    |MERGE (e:Entity {id: row.id})
    |ON CREATE SET p.id = row.id, p.property = row.property
    |""".stripMargin
input.saveAsNeo4j(opts, unwindCypher)
```