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

import org.apache.beam.sdk.io.fs.EmptyMatchTreatment
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

  it should "support EmptyMatchTreatment parameter" in {
    // Test that EmptyMatchTreatment can be set without throwing exceptions
    noException shouldBe thrownBy {
      val read = ParquetTypeSortedBucketIO
        .read[User](new TupleTag("input"))
        .from("/input")
        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)

      // Verify that the parameter is stored correctly
      read.emptyMatchTreatment shouldBe EmptyMatchTreatment.ALLOW
    }

    // Test default behavior
    val defaultRead = ParquetTypeSortedBucketIO
      .read[User](new TupleTag("input"))
      .from("/input")
    defaultRead.emptyMatchTreatment shouldBe null
  }

  it should "support all EmptyMatchTreatment values" in {
    // Test DISALLOW
    val disallowRead = ParquetTypeSortedBucketIO
      .read[User](new TupleTag("input"))
      .from("/input")
      .withEmptyMatchTreatment(EmptyMatchTreatment.DISALLOW)
    disallowRead.emptyMatchTreatment shouldBe EmptyMatchTreatment.DISALLOW

    // Test ALLOW_IF_WILDCARD
    val wildcardRead = ParquetTypeSortedBucketIO
      .read[User](new TupleTag("input"))
      .from("/input")
      .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW_IF_WILDCARD)
    wildcardRead.emptyMatchTreatment shouldBe EmptyMatchTreatment.ALLOW_IF_WILDCARD
  }

  it should "create BucketedInput with EmptyMatchTreatment" in {
    val read = ParquetTypeSortedBucketIO
      .read[User](new TupleTag("input"))
      .from("/input")
      .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)

    val bucketedInput = read.toBucketedInput(SortedBucketSource.Keying.PRIMARY)
    bucketedInput should not be null
    bucketedInput.getTupleTag shouldBe read.getTupleTag
  }

  it should "preserve EmptyMatchTreatment when chaining methods" in {
    val read = ParquetTypeSortedBucketIO
      .read[User](new TupleTag("input"))
      .from("/input")
      .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW)
      .withSuffix(".custom")

    read.emptyMatchTreatment shouldBe EmptyMatchTreatment.ALLOW
    read.getFilenameSuffix shouldBe ".custom"
  }

  it should "maintain backward compatibility with existing API usage" in {
    // Test that existing code patterns without EmptyMatchTreatment still work
    val read = ParquetTypeSortedBucketIO
      .read[User](new TupleTag("input"))
      .from("/input")
      .withSuffix(".parquet")

    read.emptyMatchTreatment shouldBe null // Default behavior preserved

    // Should still create BucketedInput successfully
    val bucketedInput = read.toBucketedInput(SortedBucketSource.Keying.PRIMARY)
    bucketedInput should not be null
  }
}
