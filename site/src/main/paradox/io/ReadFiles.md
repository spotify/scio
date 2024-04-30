# ReadFiles

Scio supports reading file paths/patterns from an `SCollection[String]` into various formats.

## Read as text lines

Reading to `String` text lines via @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles(implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[String]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val paths: SCollection[String] = ???

val lines: SCollection[String] = paths.readFiles
```

## Read entire file as String

Reading entire files to `String` via @scaladoc[readFilesAsString](com.spotify.scio.values.SCollection#readFilesAsString(implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[String]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val paths: SCollection[String] = ???

val files: SCollection[String] = paths.readFilesAsString
```

## Read entire file as binary

Reading entire files to binary `Array[Byte]` via @scaladoc[readFilesAsBytes](com.spotify.scio.values.SCollection#readFilesAsBytes(implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[Array[Byte]]):

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

val sc: ScioContext = ???
val paths: SCollection[String] = ???

val files: SCollection[Array[Byte]] = paths.readFilesAsBytes
```

## Read entire file as a custom type

Reading entire files to a custom type with a user-defined function from `FileIO.ReadableFile` to the output type via @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles[A](f:org.apache.beam.sdk.io.FileIO.ReadableFile=%3EA)(implicitevidence$24:com.spotify.scio.coders.Coder[A],implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[A]):

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

Reading a file can be done with a beam `PTransform` from a `PCollection[FileIO.ReadableFile]` to `PCollection[T]` (as an example, beam's `TextIO.readFiles()`),
via another variant of @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles[A](filesTransform:org.apache.beam.sdk.transforms.PTransform[org.apache.beam.sdk.values.PCollection[org.apache.beam.sdk.io.FileIO.ReadableFile],org.apache.beam.sdk.values.PCollection[A]],directoryTreatment:org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment,compression:org.apache.beam.sdk.io.Compression)(implicitevidence$26:com.spotify.scio.coders.Coder[A],implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[A])

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{io => beam}
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection

case class Record(i: Int, s: String)

val sc: ScioContext = ???
val paths: SCollection[String] = ???

val userTransform: PTransform[PCollection[beam.FileIO.ReadableFile], PCollection[Record]] = ???
val records: SCollection[Record] = paths.readFiles(userTransform)
```

## Read with a Beam source

Reading a file can be done with a beam `FileBasedSource[T]` (as example, beam's `TextSource`)
via another variant of @scaladoc[readFiles](com.spotify.scio.values.SCollection#readFiles[A](desiredBundleSizeBytes:Long,directoryTreatment:org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment,compression:org.apache.beam.sdk.io.Compression)(f:String=%3Eorg.apache.beam.sdk.io.FileBasedSource[A])(implicitevidence$27:com.spotify.scio.coders.Coder[A],implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[A]).

When using @scaladoc[readFilesWithPath](com.spotify.scio.values.SCollection#readFilesWithPath[A](desiredBundleSizeBytes:Long,directoryTreatment:org.apache.beam.sdk.io.FileIO.ReadMatches.DirectoryTreatment,compression:org.apache.beam.sdk.io.Compression)(filesSource:String=%3Eorg.apache.beam.sdk.io.FileBasedSource[A])(implicitevidence$29:com.spotify.scio.coders.Coder[A],implicitev:T%3C:%3CString):com.spotify.scio.values.SCollection[(String,A)]), the origin file path
will be passed along with all elements emitted by the source.

The source will be created with the given file paths, and then split in sub-ranges depending on the desired bundle size.

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.{io => beam}

case class Record(i: Int, s: String)

val sc: ScioContext = ???
val paths: SCollection[String] = ???

val desiredBundleSizeBytes: Long = ???
val directoryTreatment: beam.FileIO.ReadMatches.DirectoryTreatment = ???
val compression: beam.Compression = ???
val createSource: String => beam.FileBasedSource[Record] = ???

val records: SCollection[Record] = paths.readFiles(
  desiredBundleSizeBytes,
  directoryTreatment,
  compression
) { file => createSource(file) }

val recordsWithPath: SCollection[(String, Record)] = paths.readFilesWithPath(
  desiredBundleSizeBytes,
  directoryTreatment,
  compression
) { file => createSource(file) }
```
