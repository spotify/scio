package fix
package v0_12_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write._
import com.spotify.scio.bigquery._
import com.spotify.scio.extra.bigquery.AvroConverters.toTableSchema

object FixBqSaveAsTable {
  val tableRef = new TableReference()
  val s: Schema = null
  val wd: WriteDisposition = null
  val cd: CreateDisposition = null
  val td: String = null

  def saveAsBigQueryTable(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(Table.Ref(tableRef))

  def saveAsBigQueryTableNamedTableParam(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(table = Table.Ref(tableRef))

  def saveAsBigQueryTableMultiParamsWithoutSchema(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(Table.Ref(tableRef), writeDisposition = wd, createDisposition = cd, tableDescription = td)

  def saveAsBigQueryTableMultiParamsWithoutSchemaDiffOrder(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(Table.Ref(tableRef), createDisposition = cd, writeDisposition = wd, tableDescription = td)

  def saveAsBigQueryTableMultiParamsAllNamed(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(table = Table.Ref(tableRef), writeDisposition = wd, createDisposition = cd, tableDescription = td)

  def saveAsBigQueryTableMultiParamsWithSchemaUnnamed(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(Table.Ref(tableRef), toTableSchema(s), wd, cd, td)

  def saveAsBigQueryTableMultiParamsWithSchemaNamed(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(Table.Ref(tableRef), schema = toTableSchema(s), writeDisposition = wd, createDisposition = cd, tableDescription = td)

  def saveAsBigQueryTableMultiParamsNamedOrderChanged(in: SCollection[GenericRecord]): Unit =
    in.saveAsBigQueryTable(Table.Ref(tableRef), writeDisposition = wd, schema = toTableSchema(s))
}

