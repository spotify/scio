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
import com.spotify.scio.testing.util.ItUtils
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.util.MimeTypes

import scala.collection.JavaConverters._

class SparkeyIT extends PipelineSpec {

  import com.spotify.scio.extra.sparkey._

  val sideData = Seq(("a", "1"), ("b", "2"), ("c", "3"))

  "SCollection" should "support .asSparkeySideInput using GCS tempLocation" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)
      val tempLocation = ItUtils.gcpTempLocation("sparkey-it")
      try {
        val p1 = sc.parallelize(Seq(1))
        val p2 = sc.parallelize(sideData).asSparkeySideInput
        val s = p1.withSideInputs(p2).flatMap((_, si) => si(p2).map(_._2)).toSCollection
        s should containInAnyOrder (Seq("1", "2", "3"))
      } finally {
        val files = FileSystems
          .`match`(tempLocation + "/sparkey-*")
          .metadata().asScala.map(_.resourceId())
        FileSystems.delete(files.asJava)
      }
    }
  }

  ignore should "support repeatedly opening sparkey SideInput (#1269)" in {
    runWithContext { sc =>

      FileSystems.setDefaultPipelineOptions(sc.options)
      val tempLocation = ItUtils.gcpTempLocation("sparkey-it")
      val basePath = tempLocation + "/sparkey"
      val resourceId = FileSystems.matchNewResource(basePath + ".spl", false)
      // Create a sparkey KV file
      val uri = SparkeyUri(basePath, sc.options)
      val writer =  new SparkeyWriter(uri, -1)
      (1 to 100000000).foreach { x => writer.put(x.toString, x.toString) }
      writer.close()

      try {
        val p1 = sc.parallelize(1 to 10)
        val p2 = new SparkeyScioContext(sc).sparkeySideInput(basePath)
        p1.withSideInputs(p2)
          .map{ (x, si) =>
            si(p2).get(x.toString)
          }.toSCollection
      } finally {
        FileSystems.delete(Seq(resourceId).asJava)
      }
    }
  }

  it should "throw exception when Sparkey file exists" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)
      val tempLocation = ItUtils.gcpTempLocation("sparkey-it")
      val basePath = tempLocation + "/sparkey"
      val resourceId = FileSystems.matchNewResource(basePath + ".spi", false)
      try {
        val f = FileSystems.create(resourceId, MimeTypes.BINARY)
        f.write(ByteBuffer.wrap("test-data".getBytes))
        f.close()
        // scalastyle:off no.whitespace.before.left.bracket
        the [IllegalArgumentException] thrownBy {
          sc.parallelize(sideData).asSparkey(basePath)
        } should have message s"requirement failed: Sparkey URI $basePath already exists"
        // scalastyle:on no.whitespace.before.left.bracket
      } finally {
        FileSystems.delete(Seq(resourceId).asJava)
      }
    }
  }

}
