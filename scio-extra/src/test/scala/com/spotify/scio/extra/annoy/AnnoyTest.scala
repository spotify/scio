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

package com.spotify.scio.extra.annoy

import java.io.File
import java.nio.file.Files

import com.spotify.annoy.{ANNIndex, IndexType}
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec

class AnnoyTest extends PipelineSpec {

  val metric = Angular
  val dim = 2
  val nTrees = 10

  val sideData = Seq((1, Array(2.5f,7.2f)), (2, Array(1.2f, 2.2f)), (3, Array(5.6f, 3.4f)))

  "SCollection" should "support .asAnnoy with temporary local file" in {
    val sc = ScioContext()
    val p = sc.parallelize(sideData).asAnnoy(metric, dim, nTrees).materialize
    sc.close().waitUntilFinish()

    val path = p.waitForResult().value.next().path
    val reader = new ANNIndex(dim, path, IndexType.ANGULAR)

    sideData.foreach { s =>
      reader.getItemVector(s._1).length shouldEqual dim
      reader.getItemVector(s._1) shouldEqual s._2
    }
    new File(path).delete()
  }

  it should "support .asAnnoy with specified local file" in {
    val sc = ScioContext()
    val p = sc.parallelize(sideData).asAnnoy("test.tree", metric, dim, nTrees).materialize
    sc.close().waitUntilFinish()

    val path = p.waitForResult().value.next().path
    val reader = new ANNIndex(dim, path, IndexType.ANGULAR)

    sideData.foreach { s =>
      reader.getItemVector(s._1).length shouldEqual dim
      reader.getItemVector(s._1) shouldEqual s._2
    }
    new File(path).delete()
  }

  it should "throw exception when Annoy file already exists" in {
    val tmpDir = Files.createTempDirectory("annoy-test-")
    val path = tmpDir.resolve("annoy.tree")
    Files.createFile(path)
    // scalastyle:off no.whitespace.before.left.bracket
    the [IllegalArgumentException] thrownBy {
      runWithContext {
        _.parallelize(sideData).asAnnoy(path.toString, Angular, 40, 10)
      }
    } should have message s"requirement failed: Annoy URI $path already exists"
    // scalastyle:on no.whitespace.before.left.bracket
    Files.delete(path)
  }
}
