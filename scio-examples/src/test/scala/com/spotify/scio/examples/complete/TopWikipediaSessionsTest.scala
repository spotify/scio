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

package com.spotify.scio.examples.complete

import com.spotify.scio.bigquery.TableRow
import com.spotify.scio.testing._

class TopWikipediaSessionsTest extends PipelineSpec {
  "TopWikipediaSessions.computeTopSessions" should "work" in {
    val data = Seq(
      TableRow("timestamp" -> 0, "contributor_username" -> "user1"),
      TableRow("timestamp" -> 1, "contributor_username" -> "user1"),
      TableRow("timestamp" -> 2, "contributor_username" -> "user1"),
      TableRow("timestamp" -> 0, "contributor_username" -> "user2"),
      TableRow("timestamp" -> 1, "contributor_username" -> "user2"),
      TableRow("timestamp" -> 3601, "contributor_username" -> "user2"),
      TableRow("timestamp" -> 3602, "contributor_username" -> "user2"),
      TableRow("timestamp" -> 35 * 24 * 3600, "contributor_username" -> "user3")
    )
    val expected = Seq(
      "user1 : [1970-01-01T00:00:00.000Z..1970-01-01T01:00:02.000Z) : 3 : 1970-01-01T00:00:00.000Z",
      "user3 : [1970-02-05T00:00:00.000Z..1970-02-05T01:00:00.000Z) : 1 : 1970-02-01T00:00:00.000Z"
    )
    runWithContext { sc =>
      val p = TopWikipediaSessions.computeTopSessions(sc.parallelize(data), 1.0)
      p should containInAnyOrder(expected)
    }
  }
}
