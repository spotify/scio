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
package org.apache.beam.sdk.extensions.smb

import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.TupleTag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util.UUID

object ParquetTypeSortedBucketIOTest {
  case class User(id: Long, name: String)
}
class ParquetTypeSortedBucketIOTest extends AnyFlatSpec with Matchers {
  import ParquetTypeSortedBucketIOTest._

  "ParquetTypeSortedBucketIO" should "have serializable read" in {
    noException shouldBe thrownBy {
      SerializableUtils.ensureSerializable(
        SortedBucketIO
          .read(classOf[String])
          .of(
            ParquetTypeSortedBucketIO
              .read[User](new TupleTag("input"))
              .from("/input")
          )
      )
    }
  }

  it should "have serializable transform" in {
    noException shouldBe thrownBy {
      SerializableUtils.ensureSerializable(
        SortedBucketIO
          .read(classOf[String])
          .of(
            ParquetTypeSortedBucketIO
              .read[User](new TupleTag("input"))
              .from("/input")
          )
          .transform(
            ParquetTypeSortedBucketIO
              .transformOutput[String, User]("name")
              .to("/output")
          )
      )
    }
  }

  it should "have default temp location" in {
    val pipeline = TestPipeline.create()
    val tempDir = s"/tmp/temp-${UUID.randomUUID().toString}/"
    pipeline.getOptions.setTempLocation(tempDir)

    val write = ParquetTypeSortedBucketIO
      .write[String, User]("name")
      .to("/output")
      .withNumBuckets(1)
    val transform = SortedBucketIO
      .read(classOf[String])
      .of(
        ParquetTypeSortedBucketIO
          .read[User](new TupleTag("input"))
          .from("/input")
      )
      .transform(
        ParquetTypeSortedBucketIO
          .transformOutput[String, User]("name")
          .to("/output")
      )

    write.getTempDirectoryOrDefault(pipeline).toString shouldBe tempDir
    transform.getTempDirectoryOrDefault(pipeline).toString shouldBe tempDir
  }

  it should "check that numBuckets is set" in {
    the[IllegalArgumentException] thrownBy {
      ParquetTypeSortedBucketIO
        .write[String, User]("name")
        .to("/output")
        .expand(null)
    } should have message "numBuckets must be set to a nonzero value"
  }
}
