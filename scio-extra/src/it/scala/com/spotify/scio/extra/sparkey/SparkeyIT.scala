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

import java.nio.ByteBuffer

import com.spotify.scio.testing.PipelineSpec
import org.apache.beam.sdk.options.{GcpOptions, GcsOptions}
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.gcsfs.GcsPath

import scala.collection.JavaConverters._

class SparkeyIT extends PipelineSpec {
  import com.spotify.scio.extra.sparkey._

  val sideData = Seq(("a", "1"), ("b", "2"), ("c", "3"))

  "SCollection" should "support .asSparkeySideInput using GCS tempLocation" in {
    runWithContext { sc =>
      val tempLocation = sc.optionsAs[GcpOptions].getGcpTempLocation
      sc.options.setTempLocation(tempLocation)
      try {
        val p1 = sc.parallelize(Seq(1))
        val p2 = sc.parallelize(sideData).asSparkeySideInput
        val s = p1.withSideInputs(p2).flatMap((i, si) => si(p2).toStream.map(_._2)).toSCollection
        s should containInAnyOrder (Seq("1", "2", "3"))
      }
      finally {
        val gcs = new GcsUtilFactory().create(sc.options)
        val files = gcs.expand(GcsPath.fromUri(tempLocation + "/sparkey-*")).asScala
        gcs.remove(files.map(_.toString).asJava)
      }
    }
  }

  it should "throw exception when sparkey file exists" in {
    runWithContext { sc =>
      val tempLocation = sc.optionsAs[GcpOptions].getGcpTempLocation
      val gcs = sc.optionsAs[GcsOptions].getGcsUtil
      sc.options.setTempLocation(tempLocation)
      val sparkeyRoot = tempLocation + "/sparkey-test"
      try {
        val f = gcs.create(GcsPath.fromUri(sparkeyRoot + ".spi"), "application/octet-stream")
        f.write(ByteBuffer.wrap("test-data".getBytes))
        f.close()
        the[IllegalArgumentException] thrownBy {
          sc.parallelize(sideData).asSparkey(SparkeyUri(sparkeyRoot, sc.options))
        } should have message s"requirement failed: Sparkey URI $sparkeyRoot already exists."
      }
      finally {
        gcs.remove(Seq(sparkeyRoot + ".spi").asJava)
      }
    }
  }
}
