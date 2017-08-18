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
import com.spotify.scio.extra.annoy._

import scala.util.Random

object AnnoyExamples {
  val metric: AnnoyMetric = Angular
  val dim = 40
  val nTrees = 10

  def cosineSim(v1: Array[Float], v2: Array[Float]): Float = {
    var dp = 0f
    var ss1 = 0f
    var ss2 = 0f
    var i = 0
    while (i < v1.length) {
      dp += v1(i) * v2(i)
      ss1 += v1(i) * v1(i)
      ss2 += v2(i) * v2(i)
      i += 1
    }
    (dp / math.sqrt(ss1 * ss2)).toFloat
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.AnnoyIndexSaveExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --output=gs://[BUCKET]/[PATH]/annoy.tree
*/
object AnnoyIndexSaveExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import AnnoyExamples._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val data = (0 until 100).map(x => (x, Array.fill(40)(Random.nextFloat())))
    sc.parallelize(data).asAnnoy(args("output"), metric, dim, nTrees)

    sc.close()
  }
}

/*
SBT
runMain
  com.spotify.scio.examples.extra.AnnoySideInputExample
  --project=[PROJECT] --runner=DataflowRunner --zone=[ZONE]
  --input=gs://[BUCKET]/[PATH]/annoy.tree
  --output=gs://[BUCKET]/[PATH]/otuput
*/
object AnnoySideInputExample {
  def main(cmdlineArgs: Array[String]): Unit = {
    import AnnoyExamples._

    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // create Annoy side input from SCollection
    val data = (0 until 100).map(x => (x, Array.fill(40)(Random.nextFloat())))
    val annoySideInput = sc.parallelize(data).asAnnoySideInput(metric, dim, nTrees)

    // querying
    sc.parallelize(0 until 100).withSideInputs(annoySideInput)
      .map { (i, s) =>
        val annoyReader = s(annoySideInput)
        // get vector by item id
        val v1 = annoyReader.getItemVector(i)
        // get nearest neighbor by vector
        val results = annoyReader.getNearest(v1, 1)
        // do something with it
        val v2 = annoyReader.getItemVector(results.head)
        cosineSim(v1, v2).toString
      }
      .toSCollection
      .saveAsTextFile(args("output"))
    sc.close()
  }
}
