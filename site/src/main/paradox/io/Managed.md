# Managed IO

Beam's Managed transforms move responsibility for the creation of transform classes from user code to the runner, allowing runner-specific optimizations like hot-swapping an instance of a transform with an updated one.
Beam currently supports Iceberg and Kafka managed transforms.
See also [Dataflow's supported transforms](https://cloud.google.com/dataflow/docs/guides/managed-io).

A Scio @ref:[Coder](../internals/Coders.md) must be defined for the Beam @javadoc[Row](org.apache.beam.sdk.values.Row), derived from the Beam @javadoc[Schema](org.apache.beam.sdk.schemas.Schema) expected from the datasource.
If you have more than one type of data being read into Beam Rows, you will need to provide the coders explicitly instead of implicitly.

The source and sink parameters should be imported from Beam's @javadoc[Managed](org.apache.beam.sdk.managed.Managed).

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.managed._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.managed.Managed
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

val sc: ScioContext = ???

val rowSchema: Schema = ???
implicit val rowCoder: Coder[Row] = Coder.row(rowSchema)

val config: Map[String, Object] = ???
val rows: SCollection[Row] = sc.managed(Managed.ICEBERG, rowSchema, config)
```

Saving data to a Managed IO is similar:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.managed._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.managed.Managed
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row

val rows: SCollection[Row] = ???
val config: Map[String, Object] = ???

rows.saveAsManaged(Managed.ICEBERG, config)
```

[Magnolify's](https://github.com/spotify/magnolify) `RowType` (available as part of the `magnolify-beam` artifact) provides automatically-derived mappings between Beam's `Row` and scala case classes.

See [full documentation here](https://github.com/spotify/magnolify/blob/main/docs/beam.md) and [an example usage here](https://spotify.github.io/scio/examples/extra/ManagedExample.scala.html).

