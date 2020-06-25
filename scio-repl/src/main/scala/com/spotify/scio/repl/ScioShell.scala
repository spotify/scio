/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.repl

import com.spotify.scio.bigquery.BigQuerySysProps

/**
 * A entry-point/runner for a Scala REPL providing functionality extensions specific to working with
 * Scio.
 */
object ScioShell extends ScioGenericRunner {

  /** Runs an instance of the shell. */
  def main(args: Array[String]): Unit = {
    sys.props(BigQuerySysProps.DisableDump.flag) = "true"
    sys.props(ScioReplSysProps.MaxPrintString.flag) = "1500"

    val retVal = process(args)
    if (!retVal) {
      sys.exit(1)
    }
  }
}
