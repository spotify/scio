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
import com.spotify.scio.extra.annoy._
import com.spotify.scio.values.SCollection

/**
 * sbt "project scio-examples" "runMain com.spotify.scio.examples.extra.AnnoyIndexSaveExample
 * --runner=DataflowRunner --tempLocation=gs://<bucket>/<path> --output=<path to Annoy file>"
 */
object AnnoyIndexSaveExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val metric: ScioAnnoyMetric = Angular
    val dim = 40
    val nTrees = 10

    val r = new scala.util.Random(10)
    val data = (0 until 100).map(x => (x, Array.fill(40)(r.nextFloat())))
    val angularMain: SCollection[(Int, Array[Float])] = sc.parallelize(data)
    angularMain.asAnnoy(args("output"), metric, dim, nTrees)
    sc.close()
  }
}
