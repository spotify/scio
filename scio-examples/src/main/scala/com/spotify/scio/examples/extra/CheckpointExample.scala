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

// Example: Use Checkpoints to debug a long workflow
// Usage:

// `sbt runMain "com.spotify.scio.examples.extra.CheckpointExample
// --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
// --checkpoint=gs://[CHECKPOINT_FILE] --output=[OUTPUT]"`
package com.spotify.scio.examples.extra

import com.spotify.scio.ContextAndArgs
import com.spotify.scio.extra.checkpoint._

object CheckpointExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val words = sc.checkpoint(args("checkpoint") + "-src") {
      // Result of this block is stored as a checkpoint. If checkpoint already exists, this block
      // is not executed. In this example we use ScioContext to read an process some data.
      sc.textFile(args("input"))
        .map(_.trim)
        .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
    }
    val count = sc.checkpoint(args("checkpoint") + "-count")(
      // Another checkpoint block, this time we use SCollection, perform a transformation and
      // checkpoint the result.
      words.countByValue
    )

    words.saveAsTextFile(args("output") + "-words")
    count.max(Ordering.by(_._2)).saveAsTextFile(args("output") + "-max")
    count.map(t => t._1 + ": " + t._2).saveAsTextFile(args("output"))
    sc.close()
  }
}
