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

package com.spotify.scio.cassandra

import com.spotify.scio.testing._

class CassandraIOTest extends ScioIOSpec {
  "CassandraIO" should "work with output" in {
    val xs = 1 to 100
    def opts(query: String): CassandraOptions =
      CassandraOptions("keyspace", "table", query, "seed")
    testJobTestOutput(xs)(q => CassandraIO(opts(q))) { case (data, q) =>
      data.saveAsCassandra(opts(q))(Seq(_))
    }
  }
}
