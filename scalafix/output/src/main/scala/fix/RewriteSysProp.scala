package fix

import com.spotify.scio.bigquery.BigQueryClient
import com.spotify.scio.CoreSysProps
import com.spotify.scio.bigquery.BigQuerySysProps

object RewriteSysProp {
  BigQuerySysProps.Project.value = "project-key"
  BigQuerySysProps.CacheEnabled.value = false.toString
  BigQuerySysProps.Priority.value = "INTERACTIVE"

  val tmp = CoreSysProps.TmpDir.value
  val username = CoreSysProps.User.value
}
