package com.spotify.scio.bigquery

import com.spotify.scio.{SysProp, registerSysProps}

@registerSysProps
object BigQuerySysProps {

  val Debug = SysProp("bigquery.types.debug", "debug")

}
