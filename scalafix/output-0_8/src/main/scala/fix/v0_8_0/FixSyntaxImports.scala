package fix.v0_8_0

import com.spotify.scio.bigquery._
import com.spotify.scio.jdbc._
import com.spotify.scio.jdbc.syntax.JdbcScioContextOps
import com.spotify.scio.transforms.BaseAsyncLookupDoFn

object Ops {
  import java.util.Date
  import com.spotify.scio.jdbc.{CloudSqlOptions, JdbcConnectionOptions, JdbcReadOptions}

  def apply(date: Date,
            sc: JdbcScioContextOps,
            cloudSqlOptions: CloudSqlOptions) = ()

  def handleResponse(name: String, value: BaseAsyncLookupDoFn.Try[String]): Unit = ???
}