package fix
package v0_7_0

import com.spotify.scio.bigquery.BigQueryUtil
import com.spotify.scio.bigquery.client.BigQuery

object BQClientRefactoring {
  val bq = BigQuery.defaultInstance()

  val query = "SELECT 1"
  val table = "bigquery-public-data:samples.shakespeare"

  val sources = List("gs://data-integration-test-eu/shakespeare-sample-10.csv")
  val schema = BigQueryUtil.parseSchema(
      """
        |{
        |  "fields": [
        |    {"mode": "NULLABLE", "name": "word", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "word_count", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "corpus", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "corpus_date", "type": "INTEGER"}
        |  ]
        |}
      """.stripMargin)

  bq.query.extractLocation(query)
  bq.query.extractTables(query)
  bq.query.schema(query)
  bq.query.rows(query)
  bq.tables.schema(table)
  bq.tables.rows(table)

  val tableRef = bq.load.csv(sources, table, skipLeadingRows = 1, schema = Some(schema))
  bq.load.json(sources, table, schema = Some(schema))
  bq.load.avro(sources, table)
  tableRef.map(bq.tables.table)
}
