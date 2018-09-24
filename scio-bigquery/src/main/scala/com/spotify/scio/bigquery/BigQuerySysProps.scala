/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigquery

import com.spotify.scio.{registerSysProps, SysProp}

@registerSysProps
object BigQuerySysProps {

  val Debug = SysProp("bigquery.types.debug", "debug")

  val DisableDump = SysProp("bigquery.plugin.disable.dump", "disable class dump")

  val ClassCacheDirectory = SysProp("bigquery.class.cache.directory", "class cache directory")

  val CacheDirectory =
    SysProp("bigquery.cache.directory", "System property key for local schema cache directory")

  val CacheEnabled = SysProp("bigquery.cache.enabled",
                             "System property key for enabling or disabling scio bigquery caching")

  val Project = SysProp("bigquery.project", "System property key for billing project.")

  val Secret = SysProp("bigquery.secret", "")

  val Priority = SysProp("bigquery.priority", "\"BATCH\" or \"INTERACTIVE\"")

  val ConnectTimeoutMs = SysProp(
    "bigquery.connect_timeout",
    "Timeout in milliseconds to establish a connection. " +
      "Default is 20000 (20 seconds). 0 for an infinite timeout."
  )

  val ReadTimeoutMs = SysProp(
    "bigquery.read_timeout",
    "Timeout in milliseconds to read data from an established connection. " +
      "Default is 20000 (20 seconds). 0 for an infinite timeout."
  )

}
