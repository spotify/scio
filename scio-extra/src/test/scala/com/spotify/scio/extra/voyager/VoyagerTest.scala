/*
 * Copyright 2023 Spotify AB.
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

package com.spotify.scio.extra.voyager

import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.io.ClosedTap
import com.spotify.scio.testing.PipelineSpec
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import com.spotify.voyager.jni.StringIndex

import java.io.File
import java.nio.file.Files

class VoyagerTest extends PipelineSpec {
  val spaceType: SpaceType = SpaceType.Cosine
  val storageDataType: StorageDataType = StorageDataType.E4M3
  val dim: Int = 2

  val sideData: Seq[(String, Array[Float])] =
    Seq(("1", Array(2.5f, 7.2f)), ("2", Array(1.2f, 2.2f)), ("3", Array(5.6f, 3.4f)))

  "SCollection" should "support .asVoyager with specified local file" in {
    val tmpDir = Files.createTempDirectory("voyager-test")
    val basePath = tmpDir.toString
    val sc = ScioContext()
    val p: ClosedTap[VoyagerUri] =
      sc.parallelize(sideData)
        .asVoyager(basePath, spaceType, storageDataType, dim, 200L, 16L)
        .materialize

    val scioResult: ScioResult = sc.run().waitUntilFinish()
    val path: String = scioResult.tap(p).value.next.path

    val index: StringIndex = StringIndex.load(
      path + "/index.hnsw",
      path + "/names.json",
      SpaceType.Cosine,
      dim,
      StorageDataType.E4M3
    )

    sideData.foreach { data =>
      val result = index.query(data._2, 2, 100)
      result.getNames.length shouldEqual 2
      result.getDistances.length shouldEqual 2
      result.getNames should contain(data._1)
    }

    for (file <- Seq("index.hnsw", "names.json")) {
      new File(basePath + file).delete()
    }
  }

  it should "throw exception when the Voyager files already exists" in {
    val tmpDir = Files.createTempDirectory("voyager-test")

    val index = tmpDir.resolve("index.hnsw")
    val names = tmpDir.resolve("names.json")
    Files.createFile(index)
    Files.createFile(names)

    the[IllegalArgumentException] thrownBy {
      runWithContext {
        _.parallelize(sideData).asVoyager(
          tmpDir.toString,
          spaceType,
          storageDataType,
          dim,
          200L,
          16L
        )
      }
    } should have message s"requirement failed: Voyager URI $tmpDir already exists"

    Files.delete(index)
    Files.delete(names)
  }

}
