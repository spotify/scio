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

Both Avro [generic](https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericData.Record.html) and [specific](https://avro.apache.org/docs/current/api/java/org/apache/avro/specific/package-summary.html) records are supported when writing.

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
import com.spotify.scio.coders.Coder
import com.spotify.scio.avro._
import com.spotify.scio.parquet.avro._
import org.apache.avro.generic.GenericRecord

def input: SCollection[GenericRecord] = ???
lazy val yourAvroSchema: org.apache.avro.Schema = ???
implicit lazy val coder: Coder[GenericRecord] = avroGenericRecordCoder(yourAvroSchema)

def result = input.saveAsParquetAvroFile("gs://path-to-data/lake/output", schema = yourAvroSchema)
```

### Logical Types

As of **Scio 0.14.0** and above, Scio supports specific record logical types in parquet-avro out of the box.

When using generic record you'll need to supply the additional Configuration parameter
`AvroReadSupport.AVRO_DATA_SUPPLIER` for reads or `AvroWriteSupport.AVRO_DATA_SUPPLIER` for writes to use logical types.

```scala mdoc:compile-only
import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.parquet.avro._
import com.spotify.scio.parquet.ParquetConfiguration
import com.spotify.scio.values.SCollection
import org.apache.avro.Conversions
import org.apache.avro.generic.GenericRecord
import org.apache.avro.data.TimeConversions
import org.apache.avro.generic.GenericData
import org.apache.parquet.avro.{AvroDataSupplier, AvroReadSupport, AvroWriteSupport}

val sc: ScioContext = ???
implicit val coder: Coder[GenericRecord] = ???
val data: SCollection[GenericRecord] = ???

class AvroLogicalTypeSupplier extends AvroDataSupplier {
  override def get(): GenericData = {
    val data = GenericData.get()

    // Add conversions as needed
    data.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion())

    data
  }
}

// Reads
sc.parquetAvroFile(
  "somePath",
  conf = ParquetConfiguration.of(AvroReadSupport.AVRO_DATA_SUPPLIER -> classOf[AvroLogicalTypeSupplier])
)

// Writes
data.saveAsParquetAvroFile(
  "somePath",
  conf = ParquetConfiguration.of(AvroWriteSupport.AVRO_DATA_SUPPLIER -> classOf[AvroLogicalTypeSupplier])
)
```

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
- `fs.gs.inputstream.fadvise` - "Fadvise allows applications to provide a hint to the Linux kernel with the intended I/O access pattern, indicating how it intends to read a file, whether for sequential scans or random seeks." According to this [blog post](https://cloud.google.com/blog/products/data-analytics/new-release-of-cloud-storage-connector-for-hadoop-improving-performance-throughput-and-more) "traditional MapReduce jobs" are ideal use cases for setting this config to `SEQUENTIAL`, and Scio jobs fit in this category.

Here are some other recommended settings.

- `numShards` - This should be explicitly set so that the size of each output file is smaller than but close to `parquet.block.size`, i.e. 1 GiB. This guarantees that each file contains 1 row group only and reduces seeks.
- `compression` - Parquet defaults to ZSTD compression with a level of 3; compression level can be set to any integer from 1-22 using the configuration option `parquet.compression.codec.zstd.level`. `SNAPPY` and `GZIP` compression types also work out of the box; Snappy is less CPU intensive but has lower compression ratio. In our benchmarks GZIP seem to work better than Snappy on GCS.

A full list of Parquet configuration options can be found [here](https://github.com/apache/parquet-mr/blob/master/parquet-hadoop/README.md).

## Enabling Vectored Read API

Parquet's Java SDK added [support](https://github.com/apache/parquet-java/pull/1139) for vectored reads in 1.14.0+, configurable through the `parquet.hadoop.vectored.io.enabled` option.
This option currently defaults to false, though it's slated to be enabled by default in a future release [defaulted to true](https://github.com/apache/parquet-java/pull/3128).

Before enabling vectored read support, you must ensure that your underlying filesystem supports it as well.

### Vectored Reads for Google Cloud Storage

Google Cloud Storage has added support as of gcs-connector 3.x; however, as this version drops Java 8 support, Scio and Beam remain pinned to 2.x versions until 0.15.x and 3.x releases, respectively.

You can override your gcs-connector version to 3.x, if your pipeline meets the following conditions:

- Beam SDK is on 2.64.0+
- Scio SDK is on 0.14.19+
- Java runtime is 17+

To override gcs-connector, add the following settings to your build.sbt:

```sbt
val bigdataOssVersion = "3.1.3" // Check Maven for latest

libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % bigdataossVersion,
dependencyOverrides ++= Seq(
  "com.google.cloud.bigdataoss" % "gcsio" % bigdataossVersion,
  "com.google.cloud.bigdataoss" % "util-hadoop" % bigdataossVersion,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-auth" % hadoopVersion,
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion
)
```

## Parquet Reads in Scio 0.12.0+

Parquet read internals have been reworked in Scio 0.12.0. As of 0.12.0, you can opt-into the new Parquet read implementation,
backed by the new Beam [SplittableDoFn](https://beam.apache.org/blog/splittable-do-fn/) API, by following the instructions
@ref:[here](../releases/migrations/v0.12.0-Migration-Guide.md#parquet-reads).

## Testing

In addition to JobTest support for Avro, Typed, and Tensorflow models, Scio 0.14.5 and above include utilities for testing projections and predicates.
Just import the desired module, `com.spotify.scio.testing.parquet.{avro|types|tensorflow}._`, from the `scio-test-parquet` artifact.
For example, test utilities for Avro are available in `com.spotify.scio.testing.parquet.avro._`:

```scala
import com.spotify.scio.testing.parquet.avro._

val projection: Schema = ???
val predicate: FilterPredicate = ???

val records: Iterable[T <: SpecificRecord] = ???
val expected: Iterable[T <: SpecificRecord] = ???

records withProjection projection withPredicate predicate should contain theSameElementsAs expected
```

You can also test a case class projection against an Avro writer schema to ensure writer/reader compatibility:

```scala
import com.spotify.scio.testing.parquet.avro._

case class MyProjection(id: Int)
val records: Iterable[T <: SpecificRecord] = ???
val expected: Iterable[MyRecord] = ???

records withProjection[MyProjection] should contain theSameElementsAs expected
```
