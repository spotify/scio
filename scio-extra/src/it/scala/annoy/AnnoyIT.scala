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

import java.nio.ByteBuffer

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.util.ItUtils
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.util.MimeTypes

import scala.util.Random
import scala.collection.JavaConverters._

class AnnoyIT extends PipelineSpec {

  import com.spotify.scio.extra.annoy._

  val dim = 40
  val seed = 42
  val r = new Random(seed)
  val sideData: Seq[(Int, Array[Float])] = (0 until 10).map((_, Array.fill(dim)(r.nextFloat())))

  it should "support .asAnnoySideInput using GCS tempLocation" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)

      val tempLocation = ItUtils.gcpTempLocation("annoy-it")

      try {
        val p1 = sc.parallelize(Seq(sideData.map(_._1)))
        val p2 = sc.parallelize(sideData).asAnnoySideInput(Angular, dim, 10)
        val s = p1.withSideInputs(p2)
          .flatMap((xs, si) => xs.map(x => si(p2).getItemVector(x))).toSCollection
        s should containInAnyOrder(sideData.map(_._2))
      } finally {
        val files = FileSystems
          .`match`(s"${tempLocation}/annoy-*")
           .metadata().asScala.map(_.resourceId())
        FileSystems.delete(files.asJava)
      }
    }
  }

  it should "throw exception when Annoy file exists" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)
      val tempLocation = ItUtils.gcpTempLocation("annoy-it")
      val path = tempLocation + "/annoy.tree"
      val resourceId = FileSystems.matchNewResource(path, false)
      try {
        val f = FileSystems.create(resourceId, MimeTypes.BINARY)
        f.write(ByteBuffer.wrap("test-data".getBytes))
        f.close()
        // scalastyle:off no.whitespace.before.left.bracket
        the [IllegalArgumentException] thrownBy {
          sc.parallelize(sideData).asAnnoy(path, Angular, dim, 10)
        } should have message s"requirement failed: Annoy URI $path already exists"
        // scalastyle:on no.whitespace.before.left.bracket
      } finally {
        FileSystems.delete(Seq(resourceId).asJava)
      }
    }
  }
}
