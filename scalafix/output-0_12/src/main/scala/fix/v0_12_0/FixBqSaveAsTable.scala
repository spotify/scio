package fix.v0_12_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.extra.bigquery._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write._
import com.spotify.scio.bigquery._
import com.spotify.scio.extra.bigquery.AvroConverters.toTableSchema

object FixBqSaveAsTable {
  val tableRef = ???
  val s: Schema = ???
  val wd: WriteDisposition = ???
  val cd: CreateDisposition = ???
  val td: String = ???
  val in: SCollection[GenericRecord] = ???


    in.saveAsBigQueryTable(Table.Ref(tableRef))
    in.saveAsBigQueryTable(table = Table.Ref(tableRef))
    in.saveAsBigQueryTable(Table.Ref(tableRef), writeDisposition = wd, createDisposition = cd, tableDescription = td)
    in.saveAsBigQueryTable(Table.Ref(tableRef), createDisposition = cd, writeDisposition = wd, tableDescription = td)
    in.saveAsBigQueryTable(writeDisposition = wd, table = Table.Ref(tableRef), createDisposition = cd, tableDescription = td)
    in.saveAsBigQueryTable(Table.Ref(tableRef), toTableSchema(s), wd, cd, td)
    in.saveAsBigQueryTable(Table.Ref(tableRef), schema = toTableSchema(s), writeDisposition = wd, createDisposition = cd, tableDescription = td)
    in.saveAsBigQueryTable(Table.Ref(tableRef), writeDisposition = wd, schema = toTableSchema(s))
}

