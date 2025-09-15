# JDBC

Scio supports JDBC reads and writes.

## Reads

Reads come in two flavors: a query-based variant backed by Beam's @javadoc[JdbcIO](org.apache.beam.sdk.io.jdbc.JdbcIO) and a "sharded select" that performs a parallelizable bulk read on an entire table.

### Read via query

Query-based reads are supported with @scaladoc[jdbcSelect](com.spotify.scio.jdbc.syntax.JdbcScioContextOps#jdbcSelect[T](connectionOptions:com.spotify.scio.jdbc.JdbcConnectionOptions,query:String,statementPreparator:java.sql.PreparedStatement=%3EUnit,fetchSize:Int,outputParallelization:Boolean,dataSourceProviderFn:()=%3E,javax.sql.DataSource,configOverride:org.apache.beam.sdk.io.jdbc.JdbcIO.Read[T]=%3Eorg.apache.beam.sdk.io.jdbc.JdbcIO.Read[T])(rowMapper:java.sql.ResultSet=%3ET)(implicitevidence$3:scala.reflect.ClassTag[T],implicitevidence$4:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]).
It expects a @scaladoc[JdbcConnectionOptions](com.spotify.scio.jdbc.JdbcConnectionOptions) to connect to the database.
The `statementPreparator` argument may be used to set static parameters in the query, usually passed as arguments to the pipeline.
The curried `rowMapper` function argument maps between a `java.sql.ResultSet` to the result type `T`.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.jdbc._
import java.sql.Driver

val (sc, args): (ScioContext, Args) = ???
val sourceArg: String = args("wordCountSourceArg")

val jdbcUrl: String = ???
val driverClass: Class[Driver] = ???
val jdbcOptions = JdbcConnectionOptions("username", Some("password"), jdbcUrl, driverClass)
val query = "SELECT word, word_count FROM word_count WHERE source = ?"

val elements: SCollection[(String, Long)] = sc.jdbcSelect(jdbcOptions, query, _.setString(1, sourceArg)) { r =>
  r.getString(1) -> r.getLong(2)
}
```

### Parallelized table read

When an entire table is to be read, the input table can be sharded based on some column value and each shard read in parallel with @scaladoc[jdbcShardedSelect](com.spotify.scio.jdbc.syntax.JdbcScioContextOps#jdbcShardedSelect[T,S](readOptions:com.spotify.scio.jdbc.sharded.JdbcShardedReadOptions[T,S])(implicitevidence$3:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]).

@scaladoc[JdbcShardedReadOptions](com.spotify.scio.jdbc.sharded.JdbcShardedReadOptions) requires:

* A `rowMapper` with the same function as in `jdbcSelect`
* The `tableName` of the table to be read
* A `shardColumn`, the column on which the read will be sharded. This column must be indexed and should have an index where `shardColumn` is not part of a composite index.
* A `shard` (@scaladoc[Shard](com.spotify.scio.jdbc.sharded.Shard$)) implementation for the type of `shardColumn`. Provided implementations are `Int`, `Long`, `BigDecimal`, `Double`, `Float`, @scaladoc[ShardString.HexUpperString](com.spotify.scio.jdbc.sharded.ShardString.HexUpperString), @scaladoc[ShardString.HexLowerString](com.spotify.scio.jdbc.sharded.ShardString.HexLowerString), @scaladoc[ShardString.UuidUpperString](com.spotify.scio.jdbc.sharded.ShardString.UuidUpperString), @scaladoc[ShardString.UuidLowerString](com.spotify.scio.jdbc.sharded.ShardString.UuidLowerString), and @scaladoc[ShardString.Base64String](com.spotify.scio.jdbc.sharded.ShardString.Base64String).

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

Write to JDBC with @scaladoc[saveAsJdbc](com.spotify.scio.jdbc.syntax.JdbcSCollectionOps#saveAsJdbc(connectionOptions:com.spotify.scio.jdbc.JdbcConnectionOptions,statement:String,batchSize:Long,retryConfiguration:org.apache.beam.sdk.io.jdbc.JdbcIO.RetryConfiguration,retryStrategy:java.sql.SQLException=%3EBoolean,autoSharding:Boolean,dataSourceProviderFn:()=%3Ejavax.sql.DataSource,configOverride:org.apache.beam.sdk.io.jdbc.JdbcIO.Write[T]=%3Eorg.apache.beam.sdk.io.jdbc.JdbcIO.Write[T])(preparedStatementSetter:(T,java.sql.PreparedStatement)=%3EUnit):com.spotify.scio.io.ClosedTap[Nothing]).
It expects a @scaladoc[JdbcConnectionOptions](com.spotify.scio.jdbc.JdbcConnectionOptions) to connect to the database.
The curried `preparedStatementSetter` function argument receives an instance of the type-to-be-written and a `PreparedStatement` and appropriately sets the statement fields.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.jdbc._
import java.sql.Driver

val jdbcUrl: String = ???
val driverClass: Class[Driver] = ???
val jdbcOptions = JdbcConnectionOptions("username", Some("password"), jdbcUrl, driverClass)
val statement = "INSERT INTO word_count (word, count) values (?, ?)"

val elements: SCollection[(String, Long)] = ???
elements.saveAsJdbc(jdbcOptions, statement) { case ((word, count), statement) =>
  statement.setString(1, word)
  statement.setLong(2, count)
}
```
