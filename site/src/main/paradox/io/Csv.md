# CSV

Scio supports reading and writing _typed_ CSV via [kantan](https://nrinaudo.github.io/kantan.csv/)

Kantan provides a @scaladoc[CsvConfiguration](kantan.csv.CsvConfiguration) that allows users to configure the CSV handling, Scio's default config:

```scala
import kantan.csv._
import kantan.csv.CsvConfiguration.{Header, QuotePolicy}

CsvConfiguration(
  cellSeparator = ',',
  quote = '"',
  quotePolicy = QuotePolicy.WhenNeeded,
  header = Header.Implicit
)
```

## Read CSV

FIXME this csvFile link is incorrectly getting two $$
Reading CSV is supported via @scaladoc[csvFile](com.spotify.scio.extra.csv.syntax.ScioContextSyntax.CsvScioContext#csvFile[T](path:String,params:com.spotify.scio.extra.csv.CsvIO.ReadParam)(implicitevidence$1:kantan.csv.HeaderDecoder[T],implicitevidence$2:com.spotify.scio.coders.Coder[T]):com.spotify.scio.values.SCollection[T]).
Note that the entire file must be read into memory since CSVs are not trivially splittable.

### Read with a header

For CSV files with a header, reading requires an implicit @scaladoc[HeaderDecoder](kantan.csv.HeaderDecoder) for your type.

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.csv._
import kantan.csv._

case class A(i: Int, s: String)
implicit val decoder: HeaderDecoder[A] = HeaderDecoder.decoder("col1", "col2")(A.apply)

val sc: ScioContext = ???
val elements: SCollection[A] = sc.csvFile("gs://<input-path>/*.csv")
```

### Read without a header

For CSV files without a header, an implicit @scaladoc[RowDecoder](kantan.csv.RowDecoder) must be in scope and the read must be provided with a config specifying that there is no header:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.csv._
import kantan.csv._

case class A(i: Int, s: String)

implicit val decoder: RowDecoder[A] = RowDecoder.ordered { (col1: Int, col2: String) => A(col1, col2) }
val config = CsvIO.DefaultCsvConfiguration.withoutHeader

val sc: ScioContext = ???
val elements: SCollection[A] = sc.csvFile("gs://<input-path>/*.csv", CsvIO.ReadParam(csvConfiguration = config))
```

## Write CSV

Writing to CSV is supported via @scaladoc[saveAsCsvFile](com.spotify.scio.extra.csv.syntax.SCollectionSyntax.WritableCsvSCollection#saveAsCsvFile(path:String,suffix:String,csvConfig:kantan.csv.CsvConfiguration,numShards:Int,compression:org.apache.beam.sdk.io.Compression,shardNameTemplate:String,tempDirectory:String,filenamePolicySupplier:com.spotify.scio.util.FilenamePolicySupplier)(implicitcoder:com.spotify.scio.coders.Coder[T],implicitenc:kantan.csv.HeaderEncoder[T]):com.spotify.scio.io.ClosedTap[Nothing]).

### Write with a header

Writing with a header requires an implicit @scaladoc[HeaderEncoder](kantan.csv.HeaderEncoder) to be in scope:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.csv._
import kantan.csv._

case class A(i: Int, s: String)

implicit val encoder: HeaderEncoder[A] = HeaderEncoder.caseEncoder("col1", "col2")(A.unapply)

val elements: SCollection[A] = ???
elements.saveAsCsvFile("gs://<output-path>/")
```

### Write without a header

Writing without a header requires an implicit @scaladoc[RowEncoder](kantan.csv.RowEncoder) to be in scope:

```scala mdoc:compile-only
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import com.spotify.scio.extra.csv._
import kantan.csv._

case class A(i: Int, s: String)

implicit val encoder: RowEncoder[A] = RowEncoder.encoder(0, 1)((a: A) => (a.i, a.s))

val elements: SCollection[A] = ???
elements.saveAsCsvFile("gs://<output-path>/")
```