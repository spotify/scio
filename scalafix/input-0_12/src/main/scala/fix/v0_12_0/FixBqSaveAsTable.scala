/*
rule = FixBqSaveAsTable
*/
package fix.v0_12_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.extra.bigquery._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write._

object FixBqSaveAsTable {
  val tableRef = ???
  val s: Schema = ???
  val wd: WriteDisposition = ???
  val cd: CreateDisposition = ???
  val td: String = ???
  val in: SCollection[GenericRecord] = ???


    in.saveAvroAsBigQuery(tableRef)
    in.saveAvroAsBigQuery(table = tableRef)
    in.saveAvroAsBigQuery(tableRef, writeDisposition = wd, createDisposition = cd, tableDescription = td)
    in.saveAvroAsBigQuery(tableRef, createDisposition = cd, writeDisposition = wd, tableDescription = td)
    in.saveAvroAsBigQuery(writeDisposition = wd, table = tableRef, createDisposition = cd, tableDescription = td)
    in.saveAvroAsBigQuery(tableRef, s, wd, cd, td)
    in.saveAvroAsBigQuery(tableRef, avroSchema = s, writeDisposition = wd, createDisposition = cd, tableDescription = td)
    in.saveAvroAsBigQuery(tableRef, writeDisposition = wd, avroSchema = s)
}

