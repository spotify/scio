package com.spotify.scio.avro

import com.spotify.scio.{registerSysProps, SysProp}

@registerSysProps
object AvroSysProps {

  val Debug = SysProp("avro.types.debug", "debug")

  val DisableDump = SysProp("avro.plugin.disable.dump", "disable class dump")

  val CacheDirectory = SysProp("avro.class.cache.directory", "class cache directory")

}
