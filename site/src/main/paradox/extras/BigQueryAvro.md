# BigQueryAvro

Scio provides support for converting Avro schemas to BigQuery [`TableSchema`s](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableSchema.html) and Avro `SpecificRecord`s to a BigQuery [`TableRow`s](https://developers.google.com/resources/api-libraries/documentation/bigquery/v2/java/latest/com/google/api/services/bigquery/model/TableRow.html).

```scala mdoc:compile-only
import com.spotify.scio.extra.bigquery.AvroConverters
import org.apache.avro.specific.SpecificRecord
import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema, TableRow}

val myAvroInstance: SpecificRecord = ???
val bqSchema: TableSchema = AvroConverters.toTableSchema(myAvroInstance.getSchema)
val bqRow: TableRow = AvroConverters.toTableRow(myAvroInstance)
```
