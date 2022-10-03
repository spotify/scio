/*
rule = LintBqSaveAsTable
*/
package fix
package v0_12_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.bigquery._
import com.spotify.scio.extra.bigquery._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write._

object LintBqSaveAsTable {
  val tableRef = new TableReference()
  val schema: Schema = null
  val writeDisposition: WriteDisposition = null
  val createDisposition: CreateDisposition = null
  val tableDescription: String = null

  def saveAsBigQueryTableMultiParamsWithSchemaUnnamed(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef, schema, writeDisposition, createDisposition, tableDescription) // assert: LintBqSaveAsTable

  def saveAsBigQueryTableMultiParamsWithSchemaNamed(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef, avroSchema = schema, writeDisposition = writeDisposition, createDisposition = createDisposition, tableDescription = tableDescription) // assert: LintBqSaveAsTable

  def saveAsBigQueryTableMultiParamsUnnamed(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef, schema, writeDisposition, createDisposition, tableDescription) // assert: LintBqSaveAsTable
}
