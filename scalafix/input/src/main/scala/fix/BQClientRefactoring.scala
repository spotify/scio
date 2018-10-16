/*
rule = BQClientRefactoring
*/

package fix
package v0_7_0

import com.spotify.scio.bigquery.{ BigQueryClient, BigQueryUtil }

object BQClientRefactoring {
  val bq = BigQueryClient.defaultInstance()

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

  bq.extractLocation(query)
  bq.extractTables(query)
  bq.getQuerySchema(query)
  bq.getQueryRows(query)
  bq.getTableSchema(table)
  bq.getTableRows(table)

  val tableRef = bq.loadTableFromCsv(sources, table, skipLeadingRows = 1, schema = Some(schema))
  bq.loadTableFromJson(sources, table, schema = Some(schema))
  bq.loadTableFromAvro(sources, table)
  bq.getTable(tableRef)
}
