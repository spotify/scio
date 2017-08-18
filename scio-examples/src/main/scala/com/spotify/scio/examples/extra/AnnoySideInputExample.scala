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
 * sbt "project scio-examples" "runMain com.spotify.scio.examples.extra.AnnoySideInputExample
 * --runner=DataflowRunner --tempLocation=gs://<bucket>/<path> --input=<path to Annoy file>
 * --output=<path to output file>"
 */
object AnnoySideInputExample {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    val metric: ScioAnnoyMetric = Angular
    val dim = 40
    val nTrees = 10

    // create Annoy side input from SCollection
    val r = new scala.util.Random(10)
    val angularMain: SCollection[Int] = sc.parallelize((0 until 100))
    val annoySideInput = sc.annoySideInput(args("input"), metric, dim)

    // querying
    angularMain.withSideInputs(annoySideInput)
      .map {(i, s) =>
        val annoyIndex: AnnoyReader = s(annoySideInput)
        // get vector by item id
        val v1: Array[Float] = annoyIndex.getItemVector(i)
        // get nearest neighbor by vector
        val results: Array[Int] = annoyIndex.getNearest(v1, 1)
        // do something with it
        s"$i,${results.headOption.getOrElse("no results").toString}"
      }.toSCollection.saveAsTextFile(args("output"))
    sc.close()
  }
}
