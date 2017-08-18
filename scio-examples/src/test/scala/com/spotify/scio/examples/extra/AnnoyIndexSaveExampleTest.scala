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

import java.nio.file.Files

import com.spotify.annoy.ANNIndex
import com.spotify.scio.testing._

import scala.collection.JavaConverters._

class AnnoyIndexSaveExampleTest extends PipelineSpec {
  "AnnoyIndexSaveExample" should "work" in {
    import AnnoyExamples._

    // create index
    val tmpDir = Files.createTempDirectory("annoy-test-")
    val annoyFile = tmpDir.resolve("annoy.tree")
    AnnoyIndexSaveExample.main(Array(s"--output=$annoyFile"))

    // verify with annoy-java
    val annoyIndex = new ANNIndex(dim, annoyFile.toString)
    val vec = annoyIndex.getItemVector(0)
    val nearest = annoyIndex.getNearest(vec, 10).asScala.map(annoyIndex.getItemVector(_))
    val sims = nearest.map(cosineSim(vec, _))

    nearest.head shouldBe vec
    sims.sliding(2).forall(p => p(0) > p(1)) shouldBe true

    Files.delete(annoyFile)
    Files.delete(tmpDir)
  }
}
