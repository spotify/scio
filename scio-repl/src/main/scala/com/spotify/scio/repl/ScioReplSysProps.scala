package com.spotify.scio.repl

import com.spotify.scio.{registerSysProps, SysProp}

@registerSysProps
object ScioReplSysProps {
  val Key = SysProp("key", "")
}
