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

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.examples.common.ExampleData
import com.spotify.scio.extra.transforms._

/*
SBT
runMain
  com.spotify.scio.examples.MinimalWordCount
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://dataflow-samples/samples/misc/months.txt
  --output=gs://[BUCKET]/[PATH]/safe_flat_map
*/

object SafeFlatMapExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val (longs, errors) = sc.textFile(args.getOrElse("input", ExampleData.KING_LEAR))
      .flatMap(_.split("[^a-zA-Z0-9']+")
      .filter(_.nonEmpty))
      .safeFlatMap(e => Seq(e.toLong))

    // rescue from number format exceptions:
    val rescue = errors
      .collect {
        case (i, _: NumberFormatException) => i.length.toLong
      }

    (longs ++ rescue).sum.saveAsTextFile("num-sum")

    sc.close()
  }
}
