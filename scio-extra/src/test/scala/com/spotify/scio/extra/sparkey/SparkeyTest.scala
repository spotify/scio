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

package com.spotify.scio.extra.sparkey

import java.io.File

import com.google.common.io.Files
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.PipelineSpec
import com.spotify.sparkey.Sparkey

class SparkeyTest extends PipelineSpec {
  import com.spotify.scio.extra.sparkey._

  val sideData = Seq(("a", "1"), ("b", "2"), ("c", "3"))

  "SCollection" should "support .asSparkey with default local file" in {
    val tmpDir = Files.createTempDir().toString
    val sc = ScioContext()
    sc.options.setTempLocation(tmpDir)
    val p = sc.parallelize(sideData).asSparkey.materialize
    sc.close().waitUntilFinish()
    val basePath = p.waitForResult().value.next().basePath
    val reader = Sparkey.open(new File(basePath))
    reader.toStream.toSet shouldEqual Set(("a", "1"), ("b", "2"), ("c", "3"))
    for (ext <- Seq(".spi", ".spl")) {
      new File(basePath + ext).delete()
    }
  }

  it should "support .asSparkey with specified local file" in {
    val tmpDir = Files.createTempDir()
    val sparkeyRoot = tmpDir + "/my-sparkey-file"
    runWithContext { sc =>
      val p = sc.parallelize(sideData).asSparkey(SparkeyUri(sparkeyRoot))
    }
    val reader = Sparkey.open(new File(sparkeyRoot + ".spi"))
    reader.toStream.toSet shouldEqual Set(("a", "1"), ("b", "2"), ("c", "3"))
    for (ext <- Seq(".spi", ".spl")) {
      new File(sparkeyRoot + ext).delete()
    }
  }
}
