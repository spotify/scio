# Cassandra

Scio supports writing to [Cassandra](https://cassandra.apache.org/)

@scaladoc[saveAsCassandra](com.spotify.scio.cassandra.CassandraSCollection#saveAsCassandra(opts:com.spotify.scio.cassandra.CassandraOptions,parallelism:Int)(f:T=%3ESeq[Any]):com.spotify.scio.io.ClosedTap[Nothing]) performs bulk writes, grouping by the table partition key before writing to the cluster.

The bulk writer writes to all nodes in a cluster so remote nodes in a multi-datacenter cluster may become a bottleneck.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.cassandra._

val host: String = ???

val cql = "INSERT INTO myKeyspace.myTable (key, value1) VALUES (?, ?)"
val opts = CassandraOptions("myKeyspace", "myTable", cql, host)
val elements: SCollection[(String, Int)] = ???
elements.saveAsCassandra(opts) { case (key, value) => Seq(key, value) }
```
