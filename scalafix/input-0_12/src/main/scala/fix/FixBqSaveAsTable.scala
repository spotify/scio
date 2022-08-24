/*
rule = FixBqSaveAsTable
*/
package fix
package v0_12_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.extra.bigquery._
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord

object FixBqSaveAsTable {
  val tableRef = new TableReference()

  def saveAsBigQueryTable(in: SCollection[GenericRecord]): Unit =
    in.saveAvroAsBigQuery(tableRef)
}

