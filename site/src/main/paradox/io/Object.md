# Object file

"Object files" can be used to save an `SCollection` of records with an arbitrary type by using Beam's coder infrastructure.
Each record is encoded to a byte array by the available Beam coder, the bytes are then wrapped in a simple Avro record containing a single byte field, then saved to disk.

Object files are convenient for ad-hoc work, but it should be preferred to use a real schema-backed format when possible.

## Reading object files

Object files can be read via @scaladoc[objectFile](com.spotify.scio.avro.syntax.ScioContextOps#objectFile[T](path:String)(implicitevidence$1:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.values.SCollection

case class A(i: Int, s: String)

val sc: ScioContext = ???
val elements: SCollection[A] = sc.objectFile("gs://<input-path>/*.obj.avro")
```

## Writing object files

Object files can be written via @scaladoc[saveAsObjectFile](com.spotify.scio.avro.syntax.ObjectFileSCollectionOps#saveAsObjectFile(path:String,numShards:Int,suffix:String,codec:org.apache.avro.file.CodecFactory,metadata:Map[String,AnyRef],shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitcoder:com.spotify.scio.coders.Coder[T]):com.spotify.scio.io.ClosedTap[T]):

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.values.SCollection

case class A(i: Int, s: String)

val elements: SCollection[A] = ???
elements.saveAsObjectFile("gs://<output-path>")
```
