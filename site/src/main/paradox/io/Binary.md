# Binary

## Read Binary files

See @ref:[read as binary](ReadFiles.md#read-as-binary) for reading an entire file as a binary record.

## Write Binary files

Binary writes are supported on `SCollection[Array[Byte]]` with the @scaladoc[saveAsBinaryFile](com.spotify.scio.values.SCollection#saveAsBinaryFile(path:String,numShards:Int,prefix:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,header:Array[Byte],footer:Array[Byte],shardNameTemplate:String,framePrefix:Array[Byte]=%3EArray[Byte],frameSuffix:Array[Byte]=%3EArray[Byte],tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitev:T%3C:%3CArray[Byte]):com.spotify.scio.io.ClosedTap[Nothing]) method:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val byteArrays: SCollection[Array[Byte]] = ???
byteArrays.saveAsBinaryFile("gs://<output-dir>")
```

A static `header` and `footer` argument are provided, along with the framing parameters `framePrefix` and `frameSuffix`:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import java.nio.ByteBuffer

val byteArrays: SCollection[Array[Byte]] = ???
byteArrays.saveAsBinaryFile(
  "gs://<output-dir>",
  header = Array(1, 2, 3),
  footer = Array(4, 5, 6),
  framePrefix = arr => ByteBuffer.allocate(4).putInt(arr.length).array(),
  frameSuffix = _ => Array(0)
)
```

See also the @ref:[object file format](Object.md), which saves binary data in an avro container.
