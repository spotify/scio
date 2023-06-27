# ReadFiles

Scio supports reading file paths from an `SCollection[String]` into various formats.

## Read as text lines

Reading to `String` text lines via @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles(implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[String]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val paths: SCollection[String] = ???
val fileBytes: SCollection[String] = paths.readFiles
```

## Read entire file as String

Reading to `String` text lines via @scaladoc[readFilesAsString](com.spotify.scio.values.SCollection#readFilesAsString(implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[String]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val paths: SCollection[String] = ???
val fileBytes: SCollection[String] = paths.readFiles
```

## Read as binary

Reading to binary `Array[Byte]` via @scaladoc[readFilesAsBytes](com.spotify.scio.values.SCollection#readFilesAsBytes(implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[Array[Byte]]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val paths: SCollection[String] = ???
val fileBytes: SCollection[Array[Byte]] = paths.readFilesAsBytes
```

## Read as a custom type

Reading to a custom type with a user-defined function from `FileIO.ReadableFile` to the output type via @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles[A](f:org.apache.beam.sdk.io.FileIO.ReadableFile=%3EA)(implicitevidence$24:com.spotify.scio.coders.Coder[A],implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[A]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{io => beam}

case class A(i: Int, s: String)
val sc: ScioContext = ???
val paths: SCollection[String] = ???
val userFn: beam.FileIO.ReadableFile => A = ???
val fileBytes: SCollection[A] = paths.readFiles(userFn)
```

## Read with a Beam transform

If there is an existing beam `PTransform` from `FileIO.ReadableFile` to `A` (as an example, beam's `TextIO.readFiles()`), this can be reused via another variant of @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles[A](filesTransform:org.apache.beam.sdk.transforms.PTransform[org.apache.beam.sdk.values.PCollection[org.apache.beam.sdk.io.FileIO.ReadableFile],org.apache.beam.sdk.values.PCollection[A]],directoryTreatment:org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment,compression:org.apache.beam.sdk.io.Compression)(implicitevidence$26:com.spotify.scio.coders.Coder[A],implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[A])

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{io => beam}
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection

case class A(i: Int, s: String)

val sc: ScioContext = ???
val paths: SCollection[String] = ???
val userTransform: PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[A]] = ???
val fileBytes: SCollection[A] = paths.readFiles(userTransform)
```