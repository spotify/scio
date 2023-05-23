/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.coders

import com.google.common.collect.{ImmutableList, ImmutableMap}
import com.spotify.scio.ScioContext
import com.spotify.scio.avro._
import com.spotify.scio.avro.StringFieldTest
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import org.scalatest.BeforeAndAfterAll

import java.nio.file.Files
import scala.jdk.CollectionConverters._

class AvroCoderPipelineTest extends PipelineSpec with BeforeAndAfterAll {

  private lazy val tempFolder = {
    val dir = Files.createTempDirectory(getClass.getSimpleName)
    dir.toFile.deleteOnExit()
    dir
  }

  // Write Avro source to local file system
  override protected def beforeAll(): Unit = {
    val sc = ScioContext()
    sc
      .parallelize(1 to 10)
      .map(_ =>
        StringFieldTest
          .newBuilder()
          .setStrField("someStr")
          .setMapField(ImmutableMap.of("someKey", "someVal"))
          .setArrayField(ImmutableList.of("someListVal"))
          .build()
      )
      .saveAsAvroFile(tempFolder.toString)
    sc.run()
  }

  // Verifies fix for BEAM-12628 Avro string encoding issue
  "Avro SpecificRecords" should "be read and serialized using java String field representations" in {
    def isJavaStr(t: Any): Boolean = {
      t match {
        case (k, v) =>
          k.getClass == classOf[java.lang.String] && v.getClass == classOf[java.lang.String]
        case v: Iterable[Any] => v.forall(isJavaStr)
        case _                => t.getClass == classOf[java.lang.String]
      }
    }

    val sc = ScioContext()
    val data: SCollection[StringFieldTest] = sc
      .avroFile[StringFieldTest](tempFolder.resolve("*.avro").toString)
      .map(identity)

    data should satisfy[StringFieldTest] { mappedData =>
      mappedData.forall { record =>
        isJavaStr(record.getStrField) &&
        isJavaStr(record.getArrayField.asScala) && isJavaStr(record.getMapField.asScala)
      }
    }

    sc.run()
  }
}
