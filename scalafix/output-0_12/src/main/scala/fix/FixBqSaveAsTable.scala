package fix
package v0_12_0

import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery._
import com.google.api.services.bigquery.model.TableReference

object FixBqSaveAsTable {
  val table = new TableReference()

  def saveAsBigQueryTable(in: SCollection[Int]): Unit =
    in.saveAsBigQueryTable(Table.Ref(table))
}

