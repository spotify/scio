/*
rule = RewriteSysProp
*/

package fix
package v0_7_0

import com.spotify.scio.bigquery.BigQueryClient

object RewriteSysProp {
  sys.props(BigQueryClient.PROJECT_KEY) = "project-key"
  sys.props(BigQueryClient.CACHE_ENABLED_KEY) = false.toString
  sys.props(BigQueryClient.PRIORITY_KEY) = "INTERACTIVE"

  val tmp = sys.props("java.io.tmpdir")
  val username = sys.props("user.name")
}
