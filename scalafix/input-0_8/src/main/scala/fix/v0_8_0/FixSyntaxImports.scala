/*
rule = FixSyntaxImports
*/
package fix.v0_8_0

import com.spotify.scio.jdbc.{JdbcScioContext, JdbcSCollection}
import com.spotify.scio.bigquery.toBigQueryScioContext
import com.spotify.scio.bigquery.toBigQuerySCollection
import com.spotify.scio.transforms.AsyncLookupDoFn

object Ops {
  import java.util.Date
  import com.spotify.scio.jdbc.{CloudSqlOptions, JdbcConnectionOptions, JdbcReadOptions, JdbcScioContext}

  def apply(date: Date,
            sc: JdbcScioContext,
            cloudSqlOptions: CloudSqlOptions) = ()

  def handleResponse(name: String, value: AsyncLookupDoFn.Try[String]): Unit = ???
}
