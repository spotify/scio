/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.examples.extra

import com.spotify.scio._
import com.spotify.scio.jdbc._
import com.spotify.scio.testing._

class CloudSqlExampleTest extends PipelineSpec {
  "CloudSqlExample" should "work" in {
    val args = Array(
      "--cloudSqlUsername=john",
      "--cloudSqlPassword=secret",
      "--cloudSqlDb=mydb",
      "--cloudSqlInstanceConnectionName=project-id:zone:db-instance-name")
    val (opts, _) = ScioContext.parseArguments[CloudSqlOptions](args)
    val connOpts = CloudSqlExample.getConnectionOptions(opts)
    val readOpts = CloudSqlExample.getReadOptions(connOpts)
    val writeOpts = CloudSqlExample.getWriteOptions(connOpts)

    val input = Seq("a" -> 1L, "b" -> 2L, "c" -> 3L)
    val expected = input.map(kv => (kv._1.toUpperCase, kv._2))

    JobTest[com.spotify.scio.examples.extra.CloudSqlExample.type]
      .args(args: _*)
      .input(JdbcIO(readOpts), input)
      .output(JdbcIO[(String, Long)](writeOpts))(_ should containInAnyOrder (expected))
      .run()
  }
}
