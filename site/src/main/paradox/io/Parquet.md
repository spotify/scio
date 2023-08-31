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

### Logical Types

If your Avro schema contains a logical type, you'll need to supply an additional Configuration parameter for your reads and writes.

If you're using the default version of Avro (1.8), you can use Scio's pre-built logical type conversions:

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.values.SCollection
import com.spotify.scio.parquet.avro._
import com.spotify.scio.avro.TestRecord

val sc: ScioContext = ???
val data: SCollection[TestRecord] = sc.parallelize(List[TestRecord]())

// Reads
import com.spotify.scio.parquet.ParquetConfiguration

import org.apache.parquet.avro.AvroReadSupport

sc.parquetAvroFile(
  "somePath",
  conf = ParquetConfiguration.of(AvroReadSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier])
)

// Writes
import org.apache.parquet.avro.AvroWriteSupport

data.saveAsParquetAvroFile(
  "somePath",
  conf = ParquetConfiguration.of(AvroWriteSupport.AVRO_DATA_SUPPLIER -> classOf[LogicalTypeSupplier])
)
```

(If you're using `scio-smb`, you can use the provided class `org.apache.beam.sdk.extensions.smb.AvroLogicalTypeSupplier` instead.)

If you're using Avro 1.11, you'll have to create your own logical type supplier class, as Scio's `LogicalTypeSupplier` uses
classes present in Avro 1.8 but not 1.11. A sample Avro 1.11 logical-type supplier might look like:

```scala
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.specific.SpecificData;
import org.apache.parquet.avro.AvroDataSupplier;

case class AvroLogicalTypeSupplier() extends AvroDataSupplier {
  override def get(): GenericData = {
    val specificData = SpecificData.get()

    // Add conversions as needed
    specificData.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion())

    specificData
  }
}
```

Then, you'll have to specify your logical type supplier class in your `Configuration` as outlined above.

## Case classes

Scio uses [magnolify-parquet](https://github.com/spotify/magnolify/blob/master/docs/parquet.md) to derive Parquet reader and writer for case classes at compile time, similar to how @ref:[coders](../internals/Coders.md) work. See this [mapping table](https://github.com/spotify/magnolify/blob/master/docs/mapping.md) for how Scala and Parquet types map; enum type mapping is also specifically [documented](https://github.com/spotify/magnolify/blob/main/docs/enums.md).

### Read Parquet files as case classes

When reading Parquet files as case classes, all fields in the case class definition are read. Therefore, it's desirable to construct a case class type with only fields needed for processing.

Starting in Magnolify 0.4.8 (corresponding to Scio 0.11.6 and above), predicates for case classes have Magnolify support at the _field level only_. You can use Parquet's `FilterApi.or` and `FilterApi.and` to chain them:

```scala mdoc:reset:silent
import com.spotify.scio._
import com.spotify.scio.parquet.types._
import magnolify.parquet._
import org.apache.parquet.filter2.predicate.FilterApi

object ParquetJob {
  case class MyRecord(int_field: Int, string_field: String)

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    sc.typedParquetFile[MyRecord]("input.parquet", predicate = FilterApi.and(
      Predicate.onField[String]("string_field")(_.startsWith("a")),
      Predicate.onField[Int]("int_field")(_ % 2 == 0))
    )

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

The same Avro schema evolution principles apply to Parquet, i.e. only append `OPTIONAL` or `REPEATED` fields with default `null` or `[]`. See this [test](https://github.com/spotify/magnolify/blob/main/parquet/src/test/scala/magnolify/parquet/SchemaEvolutionSuite.scala) for some common scenarios w.r.t. Parquet schema evolution.

## Configuring Parquet

The Parquet Java [library](https://github.com/apache/parquet-mr) heavily relies on Hadoop's `Job` API. Therefore, in both the Parquet library and in scio-parquet, we use Hadoop's [Configuration](https://hadoop.apache.org/docs/r2.10.2/api/org/apache/hadoop/conf/Configuration.html) class to manage most Parquet read and write options.

The `Configuration` class, when initialized, will load default values from the first available `core-site.xml` found on the classpath. Scio-parquet provides a default [`core-site.xml` implementation](https://github.com/spotify/scio/blob/main/scio-parquet/src/main/resources/core-site.xml): if your Scio pipeline has a dependency on `scio-parquet`, these default options will be picked up in your pipeline.

### Overriding the Default Configuration

You can override the default configuration in two ways:

1. Declare a `core-site.xml` file of your own in your project's `src/main/resources` folder. Note that Hadoop can only pick one `core-site.xml` to read: if you override the file in your project, Hadoop will not read Scio's default `core-site.xml` at all, and none of its default options will be loaded.

2. Create an in-memory `Configuration` object for use with scio-parquet's `ReadParam` and `WriteParam`. Any options provided this way will be _appended_ to Scio's default configuration.

You can create and pass in a custom Configuration using our `ParquetConfiguration` helper, available in Scio 0.12.x and above:

```scala
import com.spotify.scio.parquet.ParquetConfiguration

data
  .saveAsParquetAvroFile(args("output"), conf = ParquetConfiguration.of("parquet.block.size" -> 536870912))
```

If you're on Scio 0.11.x or below, you'll have to create a `Configuration` object directly:

```scala
import org.apache.hadoop.conf.Configuration

val parquetConf: Configuration = {
  val conf: Configuration = new Configuration()
  conf.setInt("parquet.block.size", 536870912)
  conf
}
```

### Common Configuration Options

- `parquet.block.size` - This determines block size for HDFS and row group size. 1 GiB is recommended over the default 128 MiB, although you'll have to weigh the tradeoffs: a larger block size means fewer seek operations on blob storage, at the cost of having to load a larger row group into memory.
- `fs.gs.inputstream.fadvise` - Parquet relies heavily on random seeks so this GCS connector setting should be set to `RANDOM`. See this [blog post](https://cloud.google.com/blog/products/data-analytics/new-release-of-cloud-storage-connector-for-hadoop-improving-performance-throughput-and-more) for more.

Here are some other recommended settings.

- `numShards` - This should be explicitly set so that the size of each output file is smaller than but close to `parquet.block.size`, i.e. 1 GiB. This guarantees that each file contains 1 row group only and reduces seeks.
- `compression` - Parquet defaults to ZSTD compression with a level of 3; compression level can be set to any integer from 1-22 using the configuration option `parquet.compression.codec.zstd.level`. `SNAPPY` and `GZIP` compression types also work out of the box; Snappy is less CPU intensive but has lower compression ratio. In our benchmarks GZIP seem to work better than Snappy on GCS.

A full list of Parquet configuration options can be found [here](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md).

## Parquet Reads in Scio 0.12.0+

Parquet read internals have been reworked in Scio 0.12.0. As of 0.12.0, you can opt-into the new Parquet read implementation,
backed by the new Beam [SplittableDoFn](https://beam.apache.org/blog/splittable-do-fn/) API, by following the instructions
@ref:[here](../releases/migrations/v0.12.0-Migration-Guide.md#parquet-reads).
