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

import java.nio.file.{FileSystems, Files}

import com.spotify.scio.testing.PipelineSpec
import com.sun.jna.Native

class AnnoyIndexSaveExampleTest extends PipelineSpec {

  "AnnoyIndexSaveExample" should "work" in {
    import annoy4s._
    val lib = Native.loadLibrary("annoy", classOf[AnnoyLibrary])
    val r = new scala.util.Random(10)
    val data = (0 until 100).map(x => (x, Array.fill(40)(r.nextFloat())))
    val expectedIndex = lib.createAngular(40)
    data.foreach(d => lib.addItem(expectedIndex, d._1, d._2))
    lib.build(expectedIndex, 10)

    val outputFile = "actual.tree"

    try {
      JobTest[com.spotify.scio.examples.extra.AnnoyIndexSaveExample.type]
        .args(s"--output=${outputFile}")
        .run()
      val actualIndex = lib.createAngular(40)
      lib.load(actualIndex, outputFile)
      lib.getNItems(actualIndex) shouldEqual  lib.getNItems(expectedIndex)
      data.foreach{ case (i, v) =>
        val nResults = 1
        val result = Array.fill(nResults)(1)
        val distances = Array.fill(nResults)(1.0f)
        lib.getNnsByVector(expectedIndex, v, nResults, -1, result, distances)
        result(0) shouldEqual i
      }
    } finally {
      Files.deleteIfExists(FileSystems.getDefault().getPath(outputFile))
    }
  }
}
