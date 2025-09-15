# Transforms

The `com.spotify.scio.transforms` package provides a selection of transforms with additional functionality.

# WithResource

The @scaladoc[WithResource](com.spotify.scio.transforms.syntax.SCollectionWithResourceSyntax.SCollectionWithResourceFunctions) syntax provides a convenient wrapper around @scaladoc[DoFnWithResource](com.spotify.scio.transforms.DoFnWithResource) that allows reuse of some resource class, for example an API client, according to the specified @scaladoc[ResourceType](com.spotify.scio.transforms.DoFnWithResource.ResourceType) behavior for variants of `map`, `filter`, `flatMap`, and `collect`:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms._
import com.spotify.scio.transforms.DoFnWithResource.ResourceType

class Client(val name: String)
class ClientNotThreadSafe() {
  private var state: Int = 0
  def name(): String = {
    val out = s"c$state"
    state = state + 1
    out
  }
}

val elements: SCollection[String] = ???

elements.mapWithResource(new Client("c1"), ResourceType.PER_CLASS) { 
  case (client, s) => s + client.name
}
elements.filterWithResource(new Client("c2"), ResourceType.PER_INSTANCE) { 
  case (client, s) => s.nonEmpty
}
elements.collectWithResource(new Client("c3"), ResourceType.PER_INSTANCE) {
  case (client, s) if s.nonEmpty => s + client.name
}
elements.flatMapWithResource(new ClientNotThreadSafe(), ResourceType.PER_CLONE) {
  case (client, s) => s + client.name()
}
```

## Custom Parallelism

By default, a worker on dataflow batch pipeline will have a number of threads equal to the number of vCPUs.
In dataflow streaming, the [default number of threads is 300](https://github.com/apache/beam/blob/98210d99b8530346b66fcffe66b893924c910bea/runners/google-cloud-dataflow-java/worker/src/main/java/org/apache/beam/runners/dataflow/worker/StreamingDataflowWorker.java#L181).

To limit the number of concurrent items being processed a worker, @scaladoc[CustomParallelism](com.spotify.scio.transforms.syntax.SCollectionParallelismSyntax.CustomParallelismSCollection) syntax allows setting a `parallelism` argument on variants of `map`, `filter`, `flatMap`, and `collect`:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms._

val elements: SCollection[String] = ???
elements.mapWithParallelism(5) { s => s + "_append" }
elements.filterWithParallelism(5) { s => s.nonEmpty }
elements.flatMapWithParallelism(5) { s => s.split(",") }
elements.collectWithParallelism(5) { case s if s.nonEmpty => s + "_append" }
```

# FileDownload

The @scaladoc[FileDownload](com.spotify.scio.transforms.syntax.SCollectionFileDownloadSyntax.FileDownloadSCollection) syntax provides support for downloading arbitrary `URI`s to a local file, then handling the results:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms._
import scala.jdk.CollectionConverters._
import java.net.URI
import java.nio.file.Files
import java.nio.charset.StandardCharsets
  
val uris: SCollection[URI] = ???
val fileContents: SCollection[String] = uris.mapFile { path =>
  new String(Files.readAllBytes(path), StandardCharsets.UTF_8) 
}
val lines: SCollection[String] = uris.flatMapFile { path => 
  Files.readAllLines(path).asScala
}
```

# Safe flatMap

The @scaladoc[Safe](com.spotify.scio.transforms.syntax.SCollectionSafeSyntax.SpecializedFlatMapSCollection) syntax provides a `safeFlatMap` function that captures any exceptions thrown by the body of the transform and partitions its output into an `SCollection` of successfully-output elements and an `SCollection` of the exception-throwing input elements and the `Throwable` they produced.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms._

val elements: SCollection[String] = ???
val (ok: SCollection[Int], bad: SCollection[(String, Throwable)]) = elements
  .safeFlatMap { in =>
    in.split(",").map { s => s.toInt }
  }
```

# Pipe

The @scaladoc[Pipe](com.spotify.scio.transforms.syntax.SCollectionPipeSyntax.PipeSCollection) syntax provides a method to pass elements of an `SCollection[String]` to a specified command-line program.
Additional arguments allow configuration of the working directory, application environment, and setup & teardown commands.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.transforms._

val elements: SCollection[String] = ???
val upperElements: SCollection[String] = elements.pipe("tr [:lower:] [:upper:]")
```

