# Iceberg

Scio supports reading from and writing to [Apache Iceberg](https://iceberg.apache.org/) via Beam's @ref[Managed transforms](Managed.md).
[Magnolify's](https://github.com/spotify/magnolify) `RowType` (available as part of the `magnolify-beam` artifact) provides automatically-derived mappings between scala case classes and Beam's `Row`, used by the underlying managed transform. See [full documentation here](https://github.com/spotify/magnolify/blob/main/docs/beam.md).

To read:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.iceberg._
import com.spotify.scio.values.SCollection
import magnolify.beam._

case class Record(a: Int, b: String)
implicit val rt: RowType[Record] = RowType[Record]

val sc: ScioContext = ???
val table: String = ???
val catalogName: String = ???
val catalogConfig: Map[String, String] = ???

val records: SCollection[Record] = sc.iceberg[Record](
  table, 
  catalogName, 
  catalogConfig
)
```

To write:

```scala mdoc:invisible
import com.spotify.scio.iceberg._
import com.spotify.scio.values.SCollection
import magnolify.beam._
case class Record(a: Int, b: String)
implicit val rt: RowType[Record] = RowType[Record]
```

```scala mdoc:compile-only
val records: SCollection[Record] = ???

val table: String = ???
val catalogName: String = ???
val catalogConfig: Map[String, String] = ???

records.saveAsIceberg(table, catalogName, catalogConfig)
```
