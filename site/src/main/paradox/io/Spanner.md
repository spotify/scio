# Spanner

Scio supports reading and writing from [Google Cloud Spanner](https://cloud.google.com/spanner).

## Read from Spanner

Reads from Spanner occur via a query with @scaladoc[spannerQuery](com.spotify.scio.spanner.syntax.SpannerScioContextOps#spannerQuery(spannerConfig:org.apache.beam.sdk.io.gcp.spanner.SpannerConfig,query:String,withBatching:Boolean,withTransaction:Boolean):com.spotify.scio.values.SCollection[com.google.cloud.spanner.Struct]) or for an entire table with @scaladoc[spannerTable](com.spotify.scio.spanner.syntax.SpannerScioContextOps#spannerTable(spannerConfig:org.apache.beam.sdk.io.gcp.spanner.SpannerConfig,table:String,columns:Seq[String],withBatching:Boolean,withTransaction:Boolean):com.spotify.scio.values.SCollection[com.google.cloud.spanner.Struct]). Both return an `SCollection` of [`Struct`](https://www.javadoc.io/doc/com.google.cloud/google-cloud-spanner/6.38.0/com/google/cloud/spanner/Struct.html):

To read with a query:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.spanner._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import com.google.cloud.spanner.Struct

val config: SpannerConfig = SpannerConfig
    .create()
    .withProjectId("someProject")
    .withDatabaseId("someDatabase")
    .withInstanceId("someInstance")

val sc: ScioContext = ???
val queryStructs: SCollection[Struct] = sc.spannerQuery(config, "SELECT a, b FROM table WHERE c > 5")
```

To read an entire table:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.spanner._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import com.google.cloud.spanner.Struct

val config: SpannerConfig = SpannerConfig
  .create()
  .withProjectId("someProject")
  .withDatabaseId("someDatabase")
  .withInstanceId("someInstance")

val sc: ScioContext = ???
val tableStructs: SCollection[Struct] = sc.spannerTable(config, "table", columns=List("a", "b"))
```

## Write to Spanner

An `SCollection` containing [`Mutation`](https://javadoc.io/static/com.google.cloud/google-cloud-spanner/6.36.0/com/google/cloud/spanner/Mutation.html#com.google.cloud.spanner.Mutation) instances can be written to Spanner via @scaladoc[saveAsSpanner](com.spotify.scio.spanner.syntax.SpannerSCollectionOps#saveAsSpanner(spannerConfig:org.apache.beam.sdk.io.gcp.spanner.SpannerConfig,failureMode:org.apache.beam.sdk.io.gcp.spanner.SpannerIO.FailureMode,batchSizeBytes:Long):com.spotify.scio.io.ClosedTap[Nothing]):

```scala mdoc:compile-only
import com.spotify.scio.spanner._
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig
import com.google.cloud.spanner.Mutation

val config: SpannerConfig = SpannerConfig
  .create()
  .withProjectId("someProject")
  .withDatabaseId("someDatabase")
  .withInstanceId("someInstance")

val mutations: SCollection[Mutation] = ???
mutations.saveAsSpanner(config)
```

