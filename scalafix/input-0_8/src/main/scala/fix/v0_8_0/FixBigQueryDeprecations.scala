/*
rule = FixBigQueryDeprecations
*/
package fix.v0_8_0

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection

object FixBigQueryDeprecationsExample {

  val TableName = "tableSpec"

  def example[T <: TableRow](sc: ScioContext): SCollection[TableRow] = {
    sc.bigQueryTable("tableSpec").map(identity)
    sc.bigQueryTable(TableName).map(identity)
    sc.bigQueryTable(new TableReference).map(identity)
  }

}
