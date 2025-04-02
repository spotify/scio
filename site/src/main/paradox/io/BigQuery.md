# BigQuery

## Background

**NOTE that there are currently two BigQuery dialects, the [legacy query](https://cloud.google.com/bigquery/query-reference) syntax and the new [SQL 2011 standard](https://cloud.google.com/bigquery/sql-reference/). The SQL standard is highly recommended since it generates dry-run schemas consistent with actual result and eliminates a lot of edge cases when working with records in a type-safe manner. To use standard SQL, prefix your query with `#standardsql`.**

### TableRow

BigQuery rows are represented as @javadoc[TableRow](com.google.api.services.bigquery.model.TableRow) in the BigQuery Java API which is basically a `Map<String, Object>`. Fields are accessed by name strings and values must be cast or converted to the desired type, both of which are error prone process.

### Type safe BigQuery

The type safe BigQuery API in Scio represents rows as case classes and generates @javadoc[TableSchema](com.google.api.services.bigquery.model.TableSchema) converters automatically at compile time with the following mapping logic:

- Nullable fields are mapped to `Option[T]`s
- Repeated fields are mapped to `List[T]`s
- Records are mapped to nested case classes
- Timestamps are mapped to Joda Time @javadoc[Instant](org.joda.time.Instant)

See documentation for @scaladoc[BigQueryType](com.spotify.scio.bigquery.types.BigQueryType$) for the complete list of supported types.

## Type annotations

There are 5 annotations for type safe code generation.

### BigQueryType.fromStorage

This expands a class with output fields from a [BigQuery Storage API](https://cloud.google.com/bigquery/docs/reference/storage) read. Note that `class Row` has no body definition and is expanded by the annotation at compile time based on actual table schema.

Storage API provides fast access to BigQuery-managed storage by using an rpc-based protocol. It is preferred over `@BigQueryType.fromTable` and `@bigQueryType.fromQuery`. For comparison:

- `fromTable` exports the entire table to Avro files on GCS and reads from them. This incurs export cost and export quota. It can also be wasteful if only a fraction of the columns/rows are needed.
- `fromQuery` executes the query and saves result as a temporary table before reading it like `fromTable`. This incurs both query and export cost plus export quota.
- `fromStorage` accesses the underlying BigQuery storage directly, reading only columns and rows based on `selectedFields` and `rowRestriction`. No query, export cost or quota hit.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromStorage(
    "bigquery-public-data:samples.gsod",
    selectedFields = List("tornado", "month"),
    rowRestriction = "tornado = true"
  )
  class Row
```

### BigQueryType.fromTable

This expands a class with fields that map to a BigQuery table.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromTable("bigquery-public-data:samples.gsod")
class Row
```

### BigQueryType.fromQuery

This expands a class with output fields from a SELECT query. A dry run is executed at compile time to get output schema and does not incur additional cost.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromQuery("SELECT tornado, month FROM [bigquery-public-data:samples.gsod]")
class Row
```

The query string may also contain `"%s"`s and additional arguments for parameterized query. This could be handy for log type data.

```scala
import com.spotify.scio.bigquery.types.BigQueryType

// generate schema at compile time from a specific date
@BigQueryType.fromQuery("SELECT user, url FROM [my-project:logs.log_%s]", "20160101")
class Row

// generate new query strings at runtime
val newQuery = Row.query(args(0))
```

There's also a `$LATEST` placeholder for table partitions. The latest common partition for all tables with the placeholder will be used.

```scala
import com.spotify.scio.bigquery.types.BigQueryType

// generate schema at compile time from the latest date available in both my-project:log1.log_* and my-project:log2.log_*
@BigQueryType.fromQuery(
  "SELECT user, url, action FROM [my-project:log1.log_%s] JOIN [my-project:log2.log_%s] USING user",
  "$LATEST", "$LATEST")
class Row

// generate new query strings at runtime
val newQuery = Row.query(args(0), args(0))
```

### BigQueryType.fromSchema

This annotation gets schema from a string parameter and is useful in tests.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromSchema(
  """
    |{
    |  "fields": [
    |    {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"},
    |    {"mode": "REQUIRED", "name": "f2", "type": "FLOAT"},
    |    {"mode": "REQUIRED", "name": "f3", "type": "BOOLEAN"},
    |    {"mode": "REQUIRED", "name": "f4", "type": "STRING"},
    |    {"mode": "REQUIRED", "name": "f5", "type": "TIMESTAMP"}
    |    ]
    |}
  """.stripMargin)
class Row
```

### BigQueryType.toTable

This annotation works the other way around. Instead of generating class definition from a BigQuery schema, it generates BigQuery schema from a case class definition.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.toTable
case class Result(user: String, url: String, time: Long)
```

Fields in the case class and the class itself can also be annotated with `@description` which propagates to BigQuery schema.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.description

@BigQueryType.toTable
@description("A list of users mapped to the urls they visited")
case class Result(user: String,
                  url: String,
                  @description("Milliseconds since Unix epoch") time: Long)
```

### Annotation parameters

Note that due to the nature of Scala macros, only string literals and multi-line strings with optional `.stripMargin` are allowed as parameters to `BigQueryType.fromTable`, `BigQueryType.fromQuery` and `BigQueryType.fromSchema`.

These are OK:

```scala
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromTable("project-id:dataset-id.table-id")
class Row1

@BigQueryType.fromQuery(
  """
    |SELECT user, url
    |FROM [project-id:dataset-id.table-id]
  """.stripMargin)
class Row2
```

And these are not:

```scala mdoc:reset:invisible
val args: Array[String] = Array("", "*", "[project-id:dataset-id.table-id]")
```

```scala mdoc:fail
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromQuery("SELECT " + args(1) + " FROM [" + args(2) + "]")
class Row1

val sql = "SELECT " + args(1) + " FROM [" + args(2) + "]"
@BigQueryType.fromQuery(sql)
class Row2
```

### Companion objects

Classes annotated with the type safe BigQuery API have a few more convenience methods.

- `schema: TableSchema` - BigQuery schema
- `fromTableRow: (TableRow => T)` - `TableRow` to case class converter
- `toTableRow: (T => TableRow)` - case class to `TableRow` converter
- `toPrettyString(indent: Int = 0)` - pretty string representation of the schema

```scala mdoc:reset
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromTable("bigquery-public-data:samples.gsod")
class Row

Row.toPrettyString(2)
```

In addition, `BigQueryType.fromTable` and `BigQueryTable.fromQuery` generate `table: String` and `query: String` respectively that refers to parameters in the original annotation.

import com.spotify.scio.bigquery.types.BigQueryTypeUser defined companion objects may interfere with macro code generation so for now do not provide one to a case class annotated with `@BigQueryType.toTable`, i.e. `object Row`.

## Using type safe BigQuery

### Type safe BigQuery with Scio

To enable type safe BigQuery for `ScioContext`:

```scala mdoc:reset:silent
import com.spotify.scio._
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType

@BigQueryType.fromQuery("SELECT tornado, month FROM [bigquery-public-data:samples.gsod]")
class Row

@BigQueryType.toTable
case class Result(month: Long, tornado_count: Long)

def main(cmdlineArgs: Array[String]): Unit = {
  val (sc, args) = ContextAndArgs(cmdlineArgs)
  sc.typedBigQuery[Row]()  // query string from Row.query
    .flatMap(r => if (r.tornado.getOrElse(false)) Seq(r.month) else Nil)
    .countByValue
    .map(kv => Result(kv._1, kv._2))
    .saveAsTypedBigQueryTable(Table.Spec(args("output")))  // schema from Row.schema
  sc.run()
  ()
}
```

Note: Between Scio [0.14.11, 0.14.15), typed BigQuery IO used Beam's new `GenericRecord` API for BigQuery, converting
case classes into Avro records which can be loaded directly into BQ tables. However, due to regressions for certain types as well as
limited OverrideTypeProvider support, Scio 0.14.15 returns to using Beam's classic `TableRow` API for reads and writes. If you'd prefer to
use the `GenericRecord` API, which has better support for certain types like `Json`, you can optionally supply a `format` parameter to your write:

```scala mdoc:compile-only
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.BigQueryTypedTable.Format

val data: SCollection[Result] = ???
data.saveAsTypedBigQueryTable(
  Table.Spec("..."),
  format = Format.GenericRecordWithLogicalTypes
)
```

### Type safe BigQueryClient

Annotated classes can be used with the `BigQueryClient` directly too.

```scala mdoc:reset:silent
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.client.BigQuery

@BigQueryType.fromQuery("SELECT tornado, month FROM [bigquery-public-data:samples.gsod]")
class Row

def bq = BigQuery.defaultInstance()
def rows = bq.getTypedRows[Row]()
def result = bq.writeTypedRows("project-id:dataset-id.table-id", rows.toList)
```

### Using type safe BigQuery directly with Beam's IO library

If there are any BigQuery I/O operations supported in the Apache Beam client but not exposed in Scio, you may choose to use the Beam transform directly using Scio's `.saveAsCustomOutput()` option:

```scala mdoc:reset:silent
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

@BigQueryType.fromQuery("SELECT tornado, month FROM [bigquery-public-data:samples.gsod]")
class Foo

def bigQueryType = BigQueryType[Foo]
def tableRows: SCollection[Foo] = ???

def result =
  tableRows
    .map(bigQueryType.toTableRow)
    .saveAsCustomOutput(
      "custom bigquery IO",
      BigQueryIO
        .writeTableRows()
        .to("my-project:my-dataset.my-table")
        .withSchema(bigQueryType.schema)
        .withCreateDisposition(???)
        .withWriteDisposition(???)
        .withFailedInsertRetryPolicy(???)
      )
```

### BigQueryType and IntelliJ IDEA

See @ref:[the FAQ](../FAQ.md#how-to-make-intellij-idea-work-with-type-safe-bigquery-classes-) for making IntelliJ happy with type safe BigQuery.

### Custom types and validation

See @ref:[OverrideTypeProvider](../internals/OverrideTypeProvider.md) for details about the custom types and validation mechanism.

## BigQuery authentication

BigQuery authentication works a bit differently than other IO types and can be hard to reason about. File-based IOs, for example, are read from directly on each remote worker node. In contrast,
for BigQuery reads, Scio will actually launch a [Bigquery export job](https://cloud.google.com/bigquery/docs/exporting-data) from the main class process before submitting a Dataflow job request.
This export job extracts the requested BQ data to a temporary GCS location, from which the job workers can read directly from. Thus, your launcher code must be credentialed with the [required permissions](https://cloud.google.com/bigquery/docs/exporting-data#required_permissions)
to export data.

This credential will be picked up from the values of `bigquery.project` and `bigquery.secret`, if set. If they are not, Scio will attempt to find an active
[Application Default Credential](https://cloud.google.com/docs/authentication/production#automatically) and set the billing project to the value from @javadoc[DefaultProjectFactory](org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory).
As of Scio 0.11.6, you can set the SBT option `bigquery.debug_auth=true`, which enables Scio to log the active credential used in BigQuery queries that return a 403 FORBIDDEN status.

A [service account impersonation](https://cloud.google.com/iam/docs/impersonating-service-accounts) is available using SBT option `bigquery.act_as=service-account@my-project.iam.gserviceaccount.com`.
It requires `roles/iam.serviceAccountTokenCreator` to be granted to a source account.

Note that BigQuery Storage APIs don't require an export job as they can read from BigQuery directly.

## BigQuery configurations in SBT

Scio offers several BigQuery options that can be configured as SBT options - either in a root-level `.sbtopts` file or in your SBT process as `sbt -D{$OPT_KEY}=${OPT_VALUE} ...`:

| Option                            | Description                                                                                                                                                                                                                                        |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `bigquery.project`                | Specifies the billing project to use for queries. Defaults to the default project associated with the active GCP configuration (see @javadoc[DefaultProjectFactory](org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory)). |
| `bigquery.secret`                 | Specifies a file path containing a BigQuery credential. Defaults to the [Application Default Credential](https://cloud.google.com/docs/authentication/production#automatically).                                                                   |
| `bigquery.connect_timeout`        | Timeout in milliseconds to establish a connection. Default is 20000 (20 seconds). 0 for an infinite timeout.                                                                                                                                       |
| `bigquery.read_timeout`           | Timeout in milliseconds to read data from an established connection. Default is 20000 (20 seconds). 0 for an infinite timeout.                                                                                                                     |
| `bigquery.priority`               | Determines whether queries are executed in "BATCH" or "INTERACTIVE" mode. Default: BATCH.                                                                                                                                                          |
| `bigquery.debug_auth`             | Enables logging active BigQuery user information on auth errors. Default: false.                                                                                                                                                                   |
| `bigquery.types.debug`            | Enables verbose logging of macro generation steps. Default: false.                                                                                                                                                                                 |
| `bigquery.cache.enabled`          | Enables scio bigquery caching. Default: true.                                                                                                                                                                                                      |
| `generated.class.cache.directory` | BigQuery generated class cache directory. Defaults to a directory in `java.io.tmpdir`.                                                                                                                                                             |
| `bigquery.cache.directory`        | BigQuery local schema cache directory. Defaults to a directory in `java.io.tmpdir`.                                                                                                                                                                |
| `bigquery.plugin.disable.dump`    | Disable macro class dump. Default: false.                                                                                                                                                                                                          |
| `bigquery.act_as`                 | A target SA principal to impersonate current auth. Optional.                                                                                                                                                                                       |
| `bigquery.act_as_lifetime`        | A duration in seconds of a target SA temporary credentials lifetime. Default: 3600.                                                                                                                                                                |
