# Parquet

Scio supports reading and writing [Parquet](https://parquet.apache.org/) files as Avro records or Scala case classes.  Also see [[Avro]] page on reading and writing regular Avro files.

## Avro

### Read Parquet files as Avro

When reading Parquet as Avro specific records, one can use [parquet-extra](https://github.com/nevillelyh/parquet-extra) macros for generating column projections and row predicates using idiomatic Scala syntax. To read a Parquet file as Avro specific record with column projections and row predicates:

```scala
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

    sc.run()
    ()
  }
}
```

Note that the result `TestRecord`s are not complete Avro objects. Only the projected columns (`intField`, `stringField`, `booleanField`) are present while the rest are null. These objects may fail serialization and it's recommended that you map them out to tuples or case classes right after reading.

Also note that `predicate` logic is only applied when reading actual Parquet files but not in `JobTest`. To retain the filter behavior while using mock input, it's recommend that you do the following.

```scala
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

    sc.run()
    ()
  }
}
```

You can also read Avro generic records by specifying a reader schema.

```scala mdoc:reset:silent
import com.spotify.scio._
import com.spotify.scio.parquet.avro._
import com.spotify.scio.avro.TestRecord
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

object ParquetJob {
  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.parquetAvroFile[GenericRecord]("input.parquet", TestRecord.getClassSchema)
      // Map out projected fields into something type safe
      .map(r => (r.get("int_field").asInstanceOf[Int], r.get("string_field").toString))

    sc.run()
    ()
  }
}
```

### Write Avro to Parquet files

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

## Case classes

Scio uses [magnolify-parquet](https://github.com/spotify/magnolify/blob/master/docs/parquet.md) to derive Parquet reader and writer for case classes at compile time, similar to how @ref:[coders](../internals/Coders.md) work. See this [mapping table](https://github.com/spotify/magnolify/blob/master/docs/mapping.md) for how Scala and Parquet types map.

### Read Parquet files as case classes

When reading Parquet files as case classes, all fields in the case class definition are read. Therefore, it's desirable to construct a case class type with only fields needed for processing.

```scala mdoc:reset:silent
import com.spotify.scio._
import com.spotify.scio.parquet.types._

object ParquetJob {
  case class MyRecord(int_field: Int, string_field: String)

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.typedParquetFile[MyRecord]("input.parquet")

    sc.run()
    ()
  }
}
```

### Write case classes to Parquet files

When writing case classes as Parquet files, the schema is derived from the case class and all fields are written.

```scala mdoc:reset:silent
import com.spotify.scio.values._
import com.spotify.scio.parquet.types._

case class MyRecord(int_field: Int, string_field: String)
def input: SCollection[MyRecord] = ???

def result = input.saveAsTypedParquetFile("gs://path-to-data/lake/output")
```

### Compatibility

Note that Parquet writes Avro `array` fields differently than most other Parquet submodules. For example `my_field: List[Int]` would normally map to something like this:

```
repeated int32 my_field;
```

While `parquet-avro` would map it to this:

```
required group my_field {
  repeated int32 array;
}
```

Add the following import to handle typed Parquet in a way compatible with Parquet Avro:

```scala
import magnolify.parquet.ParquetArray.AvroCompat._
```

The same Avro schema evolution principles apply to Parquet, i.e. only append `OPTIONAL` or `REPEATED` fields at the end of a record with default `null` or `[]`. See this [test](https://github.com/spotify/magnolify/blob/master/parquet/src/test/scala/magnolify/parquet/test/SchemaEvolutionSuite.scala) for some common scenarios w.r.t. Parquet schema evolution.

## Performance Tuning

Some tunings might be required when writing Parquet files to maximize the read performance. Some of the Parquet settings can be configured via Hadoop [core-site.xml](https://github.com/spotify/scio/blob/master/scio-parquet/src/main/resources/core-site.xml) or `Configuration` argument.

- `parquet.block.size` - This determines block size for HDFS and row group size. 1 GiB is recommended over the default 128 MiB.
- `fs.gs.inputstream.fadvise` - Parquet relies heavily on random seeks so this GCS connector setting should be set to `RANDOM`. See this [blog post](https://cloud.google.com/blog/products/data-analytics/new-release-of-cloud-storage-connector-for-hadoop-improving-performance-throughput-and-more) for more.

Here are some other recommended settings.

- `numShards` - This should be explicitly set so that the size of each output file is smaller than but close to `parquet.block.size`, i.e. 1 GiB. This guarantees that each file contains 1 row group only and reduces seeks.
- `compression` - `SNAPPY` and `GZIP` work out of the box. Snappy is less CPU intensive but has lower compression ratio. In our benchmarks GZIP seem to work better on GCS.
