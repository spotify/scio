# JDBC

Scio supports JDBC reads and writes.

## Reads

Reads come in two flavors: a query-based variant backed by Beam's @javadoc[`JdbcIO`](org.apache.beam.sdk.io.jdbc.JdbcIO) and a "sharded select" that performs a parallelizable bulk read on an entire table.

### Read via query

Query-based reads 

@scaladoc[`jdbcSelect`](com.spotify.scio.jdbc.syntax.JdbcScioContextOps#jdbcSelect[T](readOptions:com.spotify.scio.jdbc.JdbcReadOptions[T])(implicitevidence$1:scala.reflect.ClassTag[T],implicitevidence$2:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T])

In @scaladoc[`JdbcReadOptions`](com.spotify.scio.jdbc.JdbcReadOptions), the `rowMapper` argument must be provided to map between a `java.sql.ResultSet` to the result type `T`. The `statementPreparator` argument may be used to set static parameters in the query, usually passed as arguments to the pipeline.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.jdbc._
import java.sql.Driver

val (sc, args): (ScioContext, Args) = ???
val sourceArg: String = args("wordCountSourceArg")

val jdbcUrl: String = ???
val driverClass: Class[Driver] = ???
val connOpts = JdbcConnectionOptions("username", Some("password"), jdbcUrl, driverClass)

val readOptions = JdbcReadOptions(
  connectionOptions = connOpts,
  query = "SELECT word, word_count FROM word_count WHERE source = ?",
  statementPreparator = _.setString(1, sourceArg),
  rowMapper = r => (r.getString(1), r.getLong(2))
)
val elements: SCollection[(String, Long)] = sc.jdbcSelect(readOptions)
```

### Read via sharded select

Reading data with @scaladoc[`jdbcShardedSelect`](com.spotify.scio.jdbc.syntax.JdbcScioContextOps#jdbcShardedSelect[T,S](readOptions:com.spotify.scio.jdbc.sharded.JdbcShardedReadOptions[T,S])(implicitevidence$3:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]) allows more efficient reads when an entire table is to be read.

@scaladoc[`JdbcShardedReadOptions`](com.spotify.scio.jdbc.sharded.JdbcShardedReadOptions) requires:

* A `rowMapper` with the same function as in `jdbcSelect`
* The `tableName` of the table to be read
* A `shardColumn`, the column on which the read will be sharded. This column must be indexed and should have an index where `shardColumn` is not part of a composite index.
* A `shard` (@scaladoc[Shard](com.spotify.scio.jdbc.sharded.Shard$)) implementation for the type of `shardColumn`. Provided implementations are `Int`, `Long`, `BigDecimal`, `Double`, `Float`, @scaladoc[`ShardString.HexUpperString`](com.spotify.scio.jdbc.sharded.ShardString.HexUpperString), @scaladoc[`ShardString.HexLowerString`](com.spotify.scio.jdbc.sharded.ShardString.HexLowerString), @scaladoc[`ShardString.UuidUpperString`](com.spotify.scio.jdbc.sharded.ShardString.UuidUpperString), @scaladoc[`ShardString.UuidLowerString`](com.spotify.scio.jdbc.sharded.ShardString.UuidLowerString), and @scaladoc[`ShardString.Base64String`](com.spotify.scio.jdbc.sharded.ShardString.Base64String).

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.jdbc._
import java.sql.Driver
import com.spotify.scio.jdbc.sharded._

val sc: ScioContext = ???

val jdbcUrl: String = ???
val driverClass: Class[Driver] = ???
val connOpts = JdbcConnectionOptions("username", Some("password"), jdbcUrl, driverClass)

val shardedReadOptions = JdbcShardedReadOptions[(String, Long), Long](
  connectionOptions = connOpts,
  tableName = "tableName",
  shardColumn = "word_count",
  shard = Shard.range[Long],
  rowMapper = r => (r.getString("word"), r.getLong("word_count"))
)
val elements: SCollection[(String, Long)] = sc.jdbcShardedSelect(shardedReadOptions)
```

## Writes

Write to JDBC with @scaladoc[`saveAsJdbc`](com.spotify.scio.jdbc.syntax.JdbcSCollectionOps#saveAsJdbc(writeOptions:com.spotify.scio.jdbc.JdbcWriteOptions[T]):com.spotify.scio.io.ClosedTap[Nothing]) configured with an instance of @scaladoc[`JdbcWriteOptions`](com.spotify.scio.jdbc.JdbcWriteOptions), where the `preparedStatementSetter` receives an instance of the type-to-be-written and a `PreparedStatement` and appropriately sets the statement fields:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.jdbc._
import java.sql.Driver

val jdbcUrl: String = ???
val driverClass: Class[Driver] = ???
val connOpts = JdbcConnectionOptions("username", Some("password"), jdbcUrl, driverClass)

val writeOptions = JdbcWriteOptions[(String, Long)](
  connectionOptions = connOpts,
  statement = "INSERT INTO word_count (word, count) values (?, ?)",
  preparedStatementSetter = (kv, s) => {
    s.setString(1, kv._1)
    s.setLong(2, kv._2)
  })

val elements: SCollection[(String, Long)] = ???
elements.saveAsJdbc(writeOptions)
```
