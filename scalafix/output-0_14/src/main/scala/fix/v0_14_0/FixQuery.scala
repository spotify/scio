package fix.v0_14_0

import com.spotify.scio.bigquery.types.BigQueryType.{HasQuery, Query}

object FixQuery {

  val a: HasQuery = ???
  val b: Query[_] = ???

  a.queryRaw
  b.queryRaw
}
