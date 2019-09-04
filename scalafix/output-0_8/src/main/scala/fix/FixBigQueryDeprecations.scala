package fix

import com.google.api.services.bigquery.model.TableReference
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection

object FixBigQueryDeprecationsExample {

  def example[T <: TableRow](sc: ScioContext): SCollection[TableRow] = {
    sc.bigQueryTable(Table.Spec("tableSpec")).map(identity)
    sc.bigQueryTable(Table.Ref(new TableReference)).map(identity)
  }

}
