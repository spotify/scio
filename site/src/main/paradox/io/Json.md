# Json

Scio supports reading and writing type-safe Json to a case class via [circe](https://circe.github.io/circe/).
Scio must be able to derive @scaladoc[Encoder](com.spotify.scio.extra.json#Encoder[T]=io.circe.Encoder[T]) and @scaladoc[Decoder](com.spotify.scio.extra.json#Decoder[T]=io.circe.Decoder[T]) instances for the record type.

If you need support for custom encoders or decoders, see the [circe documentation](https://circe.github.io/circe/codecs/custom-codecs.html)

## Reading Json

Read Json into a record type with @scaladoc[jsonFile](com.spotify.scio.extra.json.JsonScioContext#jsonFile[T](path:String,compression:org.apache.beam.sdk.io.Compression)(implicitevidence$1:com.spotify.scio.extra.json.package.Decoder[T],implicitevidence$2:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.json._

case class Record(i: Int, d: Double, s: String)

val sc: ScioContext = ???
val records: SCollection[Record] = sc.jsonFile[Record]("input.json")
```

## Writing Json

Write to Json with @scaladoc[saveAsJsonFile](com.spotify.scio.extra.json.JsonSCollection#saveAsJsonFile(path:String,suffix:String,numShards:Int,compression:org.apache.beam.sdk.io.Compression,printer:io.circe.Printer,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier):com.spotify.scio.io.ClosedTap[T]), which optionally takes a custom `printer` argument of type [`io.circe.Printer`](https://circe.github.io/circe/api/io/circe/Printer.html) for controlling formatting.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.extra.json._
import com.spotify.scio.values.SCollection

case class Record(i: Int, d: Double, s: String)

val elements: SCollection[Record] = ???
elements.saveAsJsonFile("gs://<output-path>")
```