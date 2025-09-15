/*
rule = BQClientRefactoring
 */

package fix.v0_7_0

import com.spotify.scio.bigquery.{BigQueryClient, BigQueryUtil}

object BQClientRefactoring {
  val bq = BigQueryClient.defaultInstance()

  val query = "SELECT 1"
  val table = "bigquery-public-data:samples.shakespeare"

  val sources = List("gs://data-integration-test-eu/shakespeare-sample-10.csv")
  val schema =
    BigQueryUtil.parseSchema("""
        |{
        |  "fields": [
        |    {"mode": "NULLABLE", "name": "word", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "word_count", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "corpus", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "corpus_date", "type": "INTEGER"}
        |  ]
        |}
      """.stripMargin)

  bq.extractLocation(query)
  bq.extractTables(query)
  bq.getQuerySchema(query)
  bq.getQueryRows(query)
  bq.getTableSchema(table)
  bq.getTableRows(table)
  bq.createTable(table, schema)
  bq.getTable(table)
  bq.getTables("projectId", "datasetId")

  bq.exportTableAsCsv(table, List())
  bq.exportTableAsJson(table, List())
  bq.exportTableAsAvro(table, List())

  val tableRefFromCsv = bq.loadTableFromCsv(sources, table, skipLeadingRows = 1, schema = Some(schema))
  bq.loadTableFromJson(sources, table, schema = Some(schema))
  bq.loadTableFromAvro(sources, table)
  bq.getTable(tableRefFromCsv)

  import org.apache.beam.sdk.io.gcp.{bigquery => beam}
  val tableRef = beam.BigQueryHelpers.parseTableSpec(table)
  bq.createTable(tableRef, schema)
}
