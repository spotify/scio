/*
rule = FixBqSaveAsTable
*/
package fix
package v0_12_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.bigquery._
import com.spotify.scio.extra.bigquery._ //TODO(farzad): needed for test, but breaks compilation
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write._

object FixBqSaveAsTable {
  val tableRef = new TableReference()
  val schema: Schema = null
  val writeDisposition: WriteDisposition = null
  val createDisposition: CreateDisposition = null
  val tableDescription: String = null

  def saveAsBigQueryTable(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef)

  def saveAsBigQueryTableNamedTableParam(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(table = tableRef)

  def saveAsBigQueryTableMultiParamsWithoutSchema(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef, writeDisposition = writeDisposition, createDisposition = createDisposition, tableDescription = tableDescription)

  def saveAsBigQueryTableMultiParamsWithoutSchemaDiffOrder(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef, createDisposition = createDisposition, writeDisposition = writeDisposition, tableDescription = tableDescription)

  def saveAsBigQueryTableMultiParamsAllNamed(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(table = tableRef, writeDisposition = writeDisposition, createDisposition = createDisposition, tableDescription = tableDescription)
}
