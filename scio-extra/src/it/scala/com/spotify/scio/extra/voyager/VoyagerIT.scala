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

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.testing.util.ItUtils
import com.spotify.scio.values.SideInput
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.util.MimeTypes

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

class VoyagerIT extends PipelineSpec {
  val dim: Int = 2
  val storageType: VoyagerStorageType = E4M3
  val distanceMeasure: VoyagerDistanceMeasure = Cosine

  val sideData: Seq[(String, Array[Float])] =
    Seq(("1", Array(2.5f, 7.2f)), ("2", Array(1.2f, 2.2f)), ("3", Array(5.6f, 3.4f)))
  it should "support .asVoyagerSideInput using GCS tempLocation" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)

      val tempLocation = ItUtils.gcpTempLocation("voyager-it")

      try {
        val p1 = sc.parallelize(sideData)
        val p2: SideInput[VoyagerReader] =
          sc.parallelize(sideData).asVoyagerSideInput(distanceMeasure, storageType, dim)
        val s = p1
          .withSideInputs(p2)
          .flatMap { (xs, si) =>
            si(p2).getNearest(xs._2, 1, 100)
          }
          .toSCollection

        s should containInAnyOrder(sideData.map(_._1))
      } finally {
        val files = FileSystems
          .`match`(s"$tempLocation")
          .metadata()
          .asScala
          .map(_.resourceId())

        FileSystems.delete(files.asJava)
      }
    }
  }

  it should "support .asVoyagerSideInput using GCS tempLocation" in {
    runWithContext { sc =>
      FileSystems.setDefaultPipelineOptions(sc.options)

      val tempLocation = ItUtils.gcpTempLocation("voyager-it")
      val namePath = tempLocation + "/names.json"
      val indexPath = tempLocation + "/index.hnsw"
      val nameResourceId = FileSystems.matchNewResource(namePath, false)
      val indexResourceId = FileSystems.matchNewResource(indexPath, false)

      try {
        val f1 = FileSystems.create(nameResourceId, MimeTypes.BINARY)
        val f2 = FileSystems.create(indexResourceId, MimeTypes.BINARY)
        f1.write(ByteBuffer.wrap("test-data".getBytes()))
        f1.close()
        f2.write(ByteBuffer.wrap("test-data".getBytes()))
        f2.close()
        the[IllegalArgumentException] thrownBy {
          sc.parallelize(sideData).asVoyager(distanceMeasure, storageType, dim)
        } should have message s""
      } finally {
        FileSystems.delete(Seq(nameResourceId, indexResourceId).asJava)
      }
    }
  }
}
