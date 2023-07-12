# Binary

## Read Binary files

See @ref:[read as binary](ReadFiles.md#read-as-binary) for reading an entire file as a single binary record.

Binary reads are supported via the @scaladoc[binaryFile](com.spotify.scio.ScioContext#binaryFile(path:String,reader:com.spotify.scio.io.BinaryIO.BinaryFileReader,compression:org.apache.beam.sdk.io.Compression,emptyMatchTreatment:org.apache.beam.sdk.io.fs.EmptyMatchTreatment,suffix:String):com.spotify.scio.values.SCollection[Array[Byte]]), with a @scaladoc[BinaryFileReader](com.spotify.scio.io.BinaryIO.BinaryFileReader) instance provided that can parse the underlying binary file format.

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.io.BinaryIO.BinaryFileReader

val sc: ScioContext = ???
val myBinaryFileReader: BinaryFileReader = ???
sc.binaryFile("gs://<input-dir>", myBinaryFileReader)
```

The complexity of the reader is determined by the complexity of the input format.
See @extref[BinaryInOut](example:BinaryInOut) for a fully-worked example.

## Write Binary files

Binary writes are supported on `SCollection[Array[Byte]]` with the @scaladoc[saveAsBinaryFile](com.spotify.scio.values.SCollection#saveAsBinaryFile(path:String,numShards:Int,prefix:String,suffix:String,compression:org.apache.beam.sdk.io.Compression,header:Array[Byte],footer:Array[Byte],shardNameTemplate:String,framePrefix:Array[Byte]=%3EArray[Byte],frameSuffix:Array[Byte]=%3EArray[Byte],tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitev:T%3C:%3CArray[Byte]):com.spotify.scio.io.ClosedTap[Nothing]) method:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection

val byteArrays: SCollection[Array[Byte]] = ???
byteArrays.saveAsBinaryFile("gs://<output-dir>")
```

A static `header` and `footer` argument are provided, along with the framing parameters `framePrefix` and `frameSuffix`. 
In this example, we record a magic number in the header along with the number of records in the file and a magic number in the footer.

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import java.nio.ByteBuffer

def intToPaddedArray(i: Int) = ByteBuffer.allocate(4).putInt(i).array()

val numRecords: Int = ???
val header = Array(1, 2, 3) ++ intToPaddedArray(numRecords)
val footer = Array(4, 5, 6)

val byteArrays: SCollection[Array[Byte]] = ???
byteArrays.saveAsBinaryFile(
  "gs://<output-dir>",
  header = header,
  footer = footer,
  framePrefix = arr => intToPaddedArray(arr.length),
  frameSuffix = _ => Array(0)
)
```

See also the @ref:[object file format](Object.md), which saves binary data in an avro container.
