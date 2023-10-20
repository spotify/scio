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
import com.spotify.voyager.jni.Index.{SpaceType, StorageDataType}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.MimeTypes

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

class VoyagerIT extends PipelineSpec {
  val space: SpaceType = SpaceType.Cosine
  val numDimensions: Int = 2
  val storageDataType: StorageDataType = StorageDataType.E4M3

  val sideData: Seq[(String, Array[Float])] = Seq(
    "1" -> Array(2.5f, 7.2f),
    "2" -> Array(1.2f, 2.2f),
    "3" -> Array(5.6f, 3.4f)
  )

  FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create())

  it should "support .asVoyagerSideInput using GCS tempLocation" in {
    val tempLocation = ItUtils.gcpTempLocation("voyager-it")
    runWithContext { sc =>
      sc.options.setTempLocation(tempLocation)
      val (names, vectors) = sideData.unzip

      val voyagerReader = sc
        .parallelize(sideData)
        .asVoyagerSideInput(
          space = space,
          numDimensions = numDimensions,
          storageDataType = storageDataType
        )

      val result = sc
        .parallelize(vectors)
        .withSideInputs(voyagerReader)
        .flatMap { case (v, ctx) =>
          ctx(voyagerReader).getNearest(v, 1, 100)
        }
        .toSCollection
        .map(_.name)

      result should containInAnyOrder(names)
    }

    // check files uploaded by voyager
    val files = FileSystems
      .`match`(s"$tempLocation/voyager-*")
      .metadata()
      .asScala
      .map(_.resourceId())

    FileSystems.delete(files.asJava)
  }

  it should "throw exception when Voyager file exists" in {
    val uri = VoyagerUri(ItUtils.gcpTempLocation("voyager-it"))
    val indexUri = uri.value.resolve(VoyagerUri.IndexFile)
    val nameUri = uri.value.resolve(VoyagerUri.NamesFile)
    val indexResourceId = FileSystems.matchNewResource(indexUri.toString, false)
    val nameResourceId = FileSystems.matchNewResource(nameUri.toString, false)

    // write some data in the
    val f1 = FileSystems.create(nameResourceId, MimeTypes.BINARY)
    val f2 = FileSystems.create(indexResourceId, MimeTypes.BINARY)
    try {
      f1.write(ByteBuffer.wrap("test-data".getBytes()))
      f2.write(ByteBuffer.wrap("test-data".getBytes()))
    } finally {
      f1.close()
      f2.close()
    }

    val e = the[IllegalArgumentException] thrownBy {
      runWithContext { sc =>
        sc.parallelize(sideData)
          .asVoyager(
            uri = uri,
            space = space,
            numDimensions = numDimensions,
            storageDataType = storageDataType
          )
      }
    }

    e.getMessage shouldBe s"requirement failed: Voyager URI ${uri.value} already exists"

    FileSystems.delete(Seq(nameResourceId, indexResourceId).asJava)
  }
}
