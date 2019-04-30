# Parquet

Scio supports reading and writing [Parquet](https://parquet.apache.org/) files as Avro records. It also includes [parquet-avro-extra](https://github.com/nevillelyh/parquet-avro-extra) macros for generating column projections and row predicates using idiomatic Scala syntax. Also see [[Avro]] page on reading and writing regular Avro files.

## Read Avro Parquet files

When reading Parquet files, only Avro specific records are supported.

To read a Parquet file with column projections and row predicates:

```scala mdoc:reset:silent
import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.avro.TestRecord

object ParquetJob {
  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Macros for generating column projections and row predicates
    val projection = Projection[TestRecord](_.getIntField, _.getLongField, _.getBooleanField)
    val predicate = Predicate[TestRecord](x => x.getIntField > 0 && x.getBooleanField)

    sc.parquetAvroFile[TestRecord]("input.parquet", projection, predicate)
      // Map out projected fields right after reading
      .map(r => (r.getIntField, r.getStringField, r.getBooleanField))

    sc.close()
    ()
  }
}
```

Note that the result `TestRecord`s are not complete Avro objects. Only the projected columns (`intField`, `stringField`, `booleanField`) are present while the rest are null. These objects may fail serialization and it's recommended that you map them out to tuples or case classes right after reading.

Also note that `predicate` logic is only applied when reading actual Parquet files but not in `JobTest`. To retain the filter behavior while using mock input, it's recommend that you do the following.

```scala mdoc:reset:silent
import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.avro.TestRecord

object ParquetJob {
  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val projection = Projection[TestRecord](_.getIntField, _.getLongField, _.getBooleanField)
    // Build both native filter function and Parquet FilterPredicate
    // case class Predicates[T](native: T => Boolean, parquet: FilterPredicate)
    val predicate = Predicate.build[TestRecord](x => x.getIntField > 0 && x.getBooleanField)

    sc.parquetAvroFile[TestRecord]("input.parquet", projection, predicate.parquet)
      // filter natively with the same logic in case of mock input in `JobTest`
      .filter(predicate.native)

    sc.close()
    ()
  }
}
```

## Write Avro Parquet files

Both Avro [generic](https://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/generic/GenericData.Record.html) and [specific](https://avro.apache.org/docs/1.8.2/api/java/org/apache/avro/specific/package-summary.html) records are supported when writing.

Type of Avro specific records will hold information about schema,
therefore Scio will figure out the schema by itself:

```scala mdoc:reset:silent
import com.spotify.scio.values._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.avro.TestRecord

def input: SCollection[TestRecord] = ???
def result = input.saveAsParquetAvroFile("gs://path-to-data/lake/output")
```

Writing Avro generic records requires additional argument `schema`:
```scala mdoc:reset:silent
import com.spotify.scio.values._
import com.spotify.scio.parquet.avro._
import org.apache.avro.generic.GenericRecord

def input: SCollection[GenericRecord] = ???
def yourAvroSchema: org.apache.avro.Schema = ???

def result = input.saveAsParquetAvroFile("gs://path-to-data/lake/output", schema = yourAvroSchema)
```
