# Text

## Reading text

Scio reads newline-delimited text via @scaladoc[textFile](com.spotify.scio.ScioContext#textFile(path:String,compression:org.apache.beam.sdk.io.Compression):com.spotify.scio.values.SCollection[String]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val elements: SCollection[String] = sc.textFile("gs://<input-path>/*.txt")
```

## Writing text

An `SCollection[String]` or `SCollection` of any class implementing `toString` can be written out to a newline-delimited text file via @scaladoc[saveAsTextFile](com.spotify.scio.values.SCollection#saveAsTextFile(path:String,numShards:Int,suffix:String,compression:org.apache.beam.sdk.io.Compression,header:Option[String],footer:Option[String],shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitct:scala.reflect.ClassTag[T]):com.spotify.scio.io.ClosedTap[String]).
An optional `header` and `footer` parameter can be provided.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.values.SCollection

val elements: SCollection[String] = ???
elements.saveAsTextFile(
  "gs://<output-path>", 
  header=Some("header"), 
  footer=Some("footer")
)
```
