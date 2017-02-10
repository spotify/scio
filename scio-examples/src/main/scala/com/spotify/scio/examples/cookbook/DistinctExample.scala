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

package com.spotify.scio.examples.cookbook

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData

/*
SBT
runMain
  com.spotify.scio.examples.cookbook.DistinctExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --gcpTempLocation=gs://[BUCKET]/path/to/staging
  --output=gs://[BUCKET]/output/path
*/

object DistinctExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val in = args.getOrElse("input", ExampleData.SHAKESPEARE_PATH)
    val out = args("output")

    sc.textFile(in)
      .distinct
      .saveAsTextFile(out)

    sc.close()
  }
}
