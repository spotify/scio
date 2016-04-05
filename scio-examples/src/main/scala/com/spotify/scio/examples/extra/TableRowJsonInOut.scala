/*
 * Copyright 2016 Spotify AB.
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
import com.spotify.scio.examples.common.ExampleData

/*
SBT
runMain
  com.spotify.scio.examples.extra.TableRowJsonInOut
  --project=[PROJECT] --runner=DataflowPipelineRunner --zone=[ZONE]
  --stagingLocation=gs://[BUCKET]/path/to/staging
  --input=gs://dataflow-samples/wikipedia_edits/wiki_data-*.json
  --output=gs://[BUCKET]/[PATH]/wikipedia
*/

// Read and write TableRowJson records
object TableRowJsonInOut {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    sc.tableRowJsonFile(args.getOrElse("input", ExampleData.EXPORTED_WIKI_TABLE))
      .take(100)
      .saveAsTableRowJsonFile(args("output"))
    sc.close()
  }
}
