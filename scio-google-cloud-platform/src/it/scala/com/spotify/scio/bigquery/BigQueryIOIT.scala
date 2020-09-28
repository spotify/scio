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

package com.spotify.scio.bigquery

import org.apache.beam.sdk.options._
import com.spotify.scio.testing._
import com.spotify.scio.testing.util.ItUtils

object BigQueryIOIT {
  @BigQueryType.fromTable("bigquery-public-data:samples.shakespeare")
  class ShakespeareFromTable

  @BigQueryType.fromQuery(
    """
    SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` LIMIT 10
  """
  )
  class ShakespeareFromQuery

  val tempLocation: String = ItUtils.gcpTempLocation("bigquery-it")
}

class BigQueryIOIT extends PipelineSpec {
  import BigQueryIOIT._
  import ItUtils.project

  val options: PipelineOptions = PipelineOptionsFactory
    .fromArgs(s"--project=$project", s"--tempLocation=$tempLocation")
    .create()

  "Select" should "read typed values from a SQL query" in
    runWithRealContext(options) { sc =>
      val scoll = sc.read(BigQueryTyped[ShakespeareFromQuery])
      scoll should haveSize(10)
      scoll should satisfy[ShakespeareFromQuery] {
        _.forall(_.getClass == classOf[ShakespeareFromQuery])
      }
    }

  "TableRef" should "read typed values from table" in
    runWithRealContext(options) { sc =>
      val scoll = sc.read(BigQueryTyped[ShakespeareFromTable])
      scoll.take(10) should haveSize(10)
      scoll should satisfy[ShakespeareFromTable] {
        _.forall(_.getClass == classOf[ShakespeareFromTable])
      }
    }
}
