# BigQuery

## Background

**NOTE that there are currently two BigQuery dialects, the [legacy query](https://cloud.google.com/bigquery/query-reference) syntax and the new [SQL 2011 standard](https://cloud.google.com/bigquery/sql-reference/). The SQL standard is highly recommended since it generates dry-run schemas consistent with actual result and eliminates a lot of edge cases when working with records in a type-safe manner. To use standard SQL, prefix your query with `#standardsql`.**

### TableRow

BigQuery rows are represented as [`TableRow`](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableRow.html) in the BigQuery Java API which is basically a `Map<String, Object>`. Fields are accessed by name strings and values must be cast or converted to the desired type, both of which are error prone process.

### Type safe BigQuery

The type safe BigQuery API in Scio represents rows as case classes and generates [`TableSchema`](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableSchema.html) converters automatically at compile time with the following mapping logic:

- Nullable fields are mapped to `Option[T]`s
- Repeated fields are mapped to `List[T]`s
- Records are mapped to nested case classes
- Timestamps are mapped to Joda Time [`Instant`](http://www.joda.org/joda-time/apidocs/org/joda/time/class-use/Instant.html)

See documentation for @scaladoc[BigQueryType](com.spotify.scio.bigquery.types.BigQueryType$) for the complete list of supported types.

## Type annotations

There are 4 annotations for type safe code generation.

### BigQueryType.fromTable

This expands a class with fields that map to a BigQuery table. Note that `class Row` has no body definition and is expanded by the annotation at compile time based on actual table schema.

```scala
@BigQueryType.fromTable("publicdata:samples.gsod")
class Row
```

### BigQueryType.fromQuery

This expands a class with output fields from a SELECT query. A dry run is executed at compile time to get output schema and does not incur additional cost.

```scala
@BigQueryType.fromQuery("SELECT tornado, month FROM [publicdata:samples.gsod]")
class Row
```

The query string may also contain `"%s"`s and additional arguments for parameterized query. This could be handy for log type data.

```scala
// generate schema at compile time from a specific date
@BigQueryType.fromQuery("SELECT user, url FROM [my-project:logs.log_%s]", "20160101")
class Row

// generate new query strings at runtime
val newQuery = Row.query.format(args(0))
```

There's also a `$LATEST` placeholder for table partitions. The latest common partition for all tables with the placeholder will be used.

```scala
// generate schema at compile time from the latest date available in both my-project:log1.log_* and my-project:log2.log_*
@BigQueryType.fromQuery(
  "SELECT user, url, action FROM [my-project:log1.log_%s] JOIN [my-project:log2.log_%s] USING user",
  "$LATEST", "$LATEST")
class Row

// generate new query strings at runtime
val newQuery = Row.query.format(args(0), args(0))
```

### BigQueryType.fromSchema

This annotation gets schema from a string parameter and is useful in tests.

```scala
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

```scala
@BigQueryType.toTable
case class Result(user: String, url: String, time: Long)
```

Fields in the case class and the class itself can also be annotated with `@description` which propagates to BigQuery schema.

```scala
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

```scala
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

```
scio> @BigQueryType.fromTable("publicdata:samples.gsod")
     | class Row
defined class Row
defined object Row

scio> println(Row.toPrettyString(2))
(
  station_number: Int,
  wban_number: Int,
  year: Int,
  month: Int,
  day: Int,
  mean_temp: Double,
  num_mean_temp_samples: Int,
  mean_dew_point: Double,
  num_mean_dew_point_samples: Int,
  mean_sealevel_pressure: Double,
  num_mean_sealevel_pressure_samples: Int,
  mean_station_pressure: Double,
  num_mean_station_pressure_samples: Int,
  mean_visibility: Double,
  num_mean_visibility_samples: Int,
  mean_wind_speed: Double,
  num_mean_wind_speed_samples: Int,
  max_sustained_wind_speed: Double,
  max_gust_wind_speed: Double,
  max_temperature: Double,
  max_temperature_explicit: Boolean,
  min_temperature: Double,
  min_temperature_explicit: Boolean,
  total_precipitation: Double,
  snow_depth: Double,
  fog: Boolean,
  rain: Boolean,
  snow: Boolean,
  hail: Boolean,
  thunder: Boolean,
  tornado: Boolean)
```
In addition, `BigQueryType.fromTable` and `BigQueryTable.fromQuery` generate `table: String` and `query: String` respectively that refers to parameters in the original annotation.

User defined companion objects may interfere with macro code generation so for now do not provide one to a case class annotated with `@BigQueryType.toTable`, i.e. `object Row`.

## Using type safe BigQuery

### Type safe BigQuery with Scio

To enable type safe BigQuery for `ScioContext`:

```scala
import com.spotify.scio.bigquery._

@BigQueryType.fromQuery("SELECT tornado, month FROM [publicdata:samples.gsod]")
class Row

@BigQueryType.toTable
case class Result(month: Long, tornado_count: Long)

def main(cmdlineArgs: Array[String]): Unit = {
  val (sc, args) = ContextAndArgs(cmdlineArgs)
  sc.typedBigQuery[Row]()  // query string from Row.query
    .flatMap(r => if (r.tornado.getOrElse(false)) Seq(r.month) else Nil)
    .countByValue
    .map(kv => Result(kv._1, kv._2))
    .saveAsTypedBigQuery(args("output"))  // schema from Row.schema
  sc.close()
()
}
```

### Type safe BigQueryClient

Annotated classes can be used with the `BigQueryClient` directly too.

```scala
import com.spotify.scio.bigquery.client.BigQuery

val bq = new BigQuery()
val rows = bq.getTypedRows[Row]()
bq.writeTypedRows("project-id:dataset-id.table-id", rows.toList)
```

### Using type safe BigQuery directly with Beam's IO library

If there are any BigQuery I/O operations supported in the Apache Beam client but not exposed in Scio, you may choose to use the Beam transform directly using Scio's `.saveAsCustomOutput()` option:

```scala
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO

val bigQueryType = BigQueryType[Foo]
val tableRows: SCollection[Foo] = ...

tableRows
  .map(bigQueryType.toTableRow)
  .saveAsCustomOutput(
    "custom bigquery IO",
    BigQueryIO
      .writeTableRows()
      .to("my-project:my-dataset.my-table")
      .withSchema(bigQueryType.schema)
      .withCreateDisposition(...)
      .withWriteDisposition(...)
      .withFailedInsertRetryPolicy(...)
    )
```

### BigQueryType and IntelliJ IDEA

See @ref[the FAQ](../FAQ.md#how-to-make-intellij-idea-work-with-type-safe-bigquery-classes-) for making IntelliJ happy with type safe BigQuery.

### Custom types and validation

See @ref[OverrideTypeProvider](../internals/OverrideTypeProvider.md) for details about the custom types and validation mechanism.
