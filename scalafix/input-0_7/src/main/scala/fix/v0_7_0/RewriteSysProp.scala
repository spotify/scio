/*
rule = RewriteSysProp
*/

package fix.v0_7_0

import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.bigquery.BigQueryClient.PROJECT_KEY

object RewriteSysProp {
  sys.props(BigQueryClient.PROJECT_KEY) = "project-key"
  sys.props(BigQueryClient.CACHE_ENABLED_KEY) = false.toString
  sys.props(BigQueryClient.PRIORITY_KEY) = "INTERACTIVE"

  sys.props(PROJECT_KEY) = "project-key"

  val tmp = sys.props("java.io.tmpdir")
  val username = sys.props("user.name")
}
