/*
rule = FixQuery
 */
package fix.v0_14_0

import com.spotify.scio.bigquery.types.BigQueryType.HasQuery

// Workaround to get 0.14 code to compile (query method has been removed from HasQuery)
trait HasQueryRaw {
  def query: String = ???
}

object HasQueryType extends HasQuery with HasQueryRaw {
  override def queryRaw: String = ???
  override def query: String = ???
}

object FixQuery {
  HasQueryType.query

  HasQueryType.query.format("foo")
}
