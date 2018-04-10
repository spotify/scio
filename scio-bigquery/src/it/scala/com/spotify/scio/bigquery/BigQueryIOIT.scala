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

import java.util.UUID

import com.spotify.scio.testing._
import com.spotify.scio.bigquery.nio._

object BigQueryIOIT {

  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class ShakespeareFromTable

  @BigQueryType.fromQuery("""
    SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10
  """)
  class ShakespeareFromQuery

  val tempLocation = com.spotify.scio.testing.util.ItUtils.gcpTempLocation("bigquery-it")
}

class BigQueryIOIT extends PipelineSpec {
  import BigQueryIOIT._

  "Select" should "read typed values from a sql query" in
    runWithContext(tempLocation) { sc =>
      val scoll = sc.read(Typed[ShakespeareFromQuery])
      scoll should haveSize (10)
      scoll should satisfy[ShakespeareFromQuery] {
        _.forall(_.getClass == classOf[ShakespeareFromQuery])
      }
    }

  "TableRef" should "read typed values from table" in
    runWithContext(tempLocation) { sc =>
      val scoll = sc.read(Typed[ShakespeareFromTable])
      scoll.take(10) should haveSize (10)
      scoll should satisfy[ShakespeareFromTable] {
        _.forall(_.getClass == classOf[ShakespeareFromTable])
      }
    }
}
