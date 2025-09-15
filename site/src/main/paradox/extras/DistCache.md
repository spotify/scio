# DistCache

Scio supports a distributed cache, @scaladoc[DistCache](com.spotify.scio.values.DistCache), that is similar to Hadoop's. 

A set of one or more paths that back the DistCache are lazily downloaded by all workers, then passed through a user-defined initialization function `initFn` to be deserialized into an in-memory representation that can be used by all threads on that worker.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import org.joda.time.Instant
import java.io.File

val sc: ScioContext = ???
val uri: String = ???
def parseFn(file: File): Map[String, String] = ???

val dc = sc.distCache(uri) { file => parseFn(file) }

val elements: SCollection[String] = ???
elements.flatMap { e =>
  val optResult = dc().get(e)
  optResult
}
```

See @extref[DistCacheExample.scala](example:DistCacheExample).
