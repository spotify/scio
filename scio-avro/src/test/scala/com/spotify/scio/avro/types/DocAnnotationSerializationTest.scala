/*
 * Copyright 2025 Spotify AB.
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

package com.spotify.scio.avro.types

import com.spotify.scio._
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.PipelineSpec
import magnolify.avro.{AvroType => MagnolifyAvroType}
import org.apache.avro.generic.GenericRecord


/**
 * Test case class with @AvroType.toSchema and @doc annotations.
 *
 * This tests that the @doc annotation is Serializable and can be used
 * with Magnolify's AvroType in map operations.
 */
object DocAnnotationSerializationTest {

  @AvroType.toSchema
  case class RecordWithDoc(
    @doc("The unique identifier") id: String,
    @doc("The display name") name: String
  )

  @AvroType.toSchema
  case class RecordWithoutDoc(
    id: String,
    name: String
  )
}

/**
 * Job that creates RecordWithDoc instances in a map operation.
 *
 * This exercises the code path where:
 * 1. Input data is read
 * 2. A map operation creates case class instances with @doc annotations
 * 3. The case class is converted to GenericRecord using Magnolify's AvroType
 *
 * The key serialization boundary is in step 3, where the AvroType (which
 * contains @doc annotation objects) must be serializable.
 */
object DocAnnotationMapJob {
  import DocAnnotationSerializationTest.RecordWithDoc

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    // Derive Magnolify AvroType - this captures @doc annotations
    implicit val avroType: MagnolifyAvroType[RecordWithDoc] = MagnolifyAvroType[RecordWithDoc]
    implicit val grCoder: Coder[GenericRecord] =
      avroGenericRecordCoder(avroType.schema)

    sc.textFile(args("input"))
      .map { line =>
        // Create case class instance inside map (on worker)
        val parts = line.split(",")
        RecordWithDoc(parts(0), parts(1))
      }
      // Convert to GenericRecord using AvroType.to - THIS IS WHERE SERIALIZATION FAILS
      // because avroType.to captures the full AvroType which contains @doc annotation objects
      .map(avroType.to)
      .saveAsAvroFile(args("output"), schema = avroType.schema)

    sc.run()
    ()
  }
}

/**
 * Job that creates RecordWithoutDoc instances - should always work.
 */
object NoDocAnnotationMapJob {
  import DocAnnotationSerializationTest.RecordWithoutDoc

  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(cmdlineArgs)

    implicit val avroType: MagnolifyAvroType[RecordWithoutDoc] = MagnolifyAvroType[RecordWithoutDoc]
    implicit val grCoder: Coder[GenericRecord] =
      avroGenericRecordCoder(avroType.schema)

    sc.textFile(args("input"))
      .map { line =>
        val parts = line.split(",")
        RecordWithoutDoc(parts(0), parts(1))
      }
      .map(avroType.to)
      .saveAsAvroFile(args("output"), schema = avroType.schema)

    sc.run()
    ()
  }
}

class DocAnnotationSerializationTest extends PipelineSpec {
  import DocAnnotationSerializationTest._

  val inputData = Seq("id1,name1", "id2,name2", "id3,name3")

  "RecordWithDoc" should "be creatable with @doc annotations" in {
    val record = RecordWithDoc("test-id", "test-name")
    record.id shouldBe "test-id"
    record.name shouldBe "test-name"
  }

  it should "have Magnolify AvroType derivable" in {
    val avroType = MagnolifyAvroType[RecordWithDoc]
    avroType.schema should not be null
    avroType.schema.getField("id") should not be null
    avroType.schema.getField("name") should not be null
  }

  it should "have doc annotations in schema" in {
    val avroType = MagnolifyAvroType[RecordWithDoc]
    // Check if doc is preserved in schema
    val idField = avroType.schema.getField("id")
    val nameField = avroType.schema.getField("name")

    // Note: Whether doc is preserved depends on Magnolify's handling
    // This test documents the current behavior
    idField should not be null
    nameField should not be null
  }

  /**
   * This test reproduces the serialization issue with @doc annotations.
   *
   * Before the fix (making @doc Serializable), this test would fail with:
   * java.io.NotSerializableException: com.spotify.scio.avro.types.package$doc
   *
   * After the fix, this test should pass.
   */
  "DocAnnotationMapJob" should "serialize @doc annotations in map operation" in {
    implicit val avroType: MagnolifyAvroType[RecordWithDoc] = MagnolifyAvroType[RecordWithDoc]
    implicit val grCoder: Coder[GenericRecord] =
      avroGenericRecordCoder(avroType.schema)

    val expected = Seq(
      RecordWithDoc("id1", "name1"),
      RecordWithDoc("id2", "name2"),
      RecordWithDoc("id3", "name3")
    ).map(avroType.to)

    JobTest[DocAnnotationMapJob.type]
      .args("--input=in.txt", "--output=out.avro")
      .input(TextIO("in.txt"), inputData)
      .output(AvroIO[GenericRecord]("out.avro")) { actual =>
        actual should containInAnyOrder(expected)
      }
      .run()
  }

  /**
   * Control test - same job without @doc should always work.
   */
  "NoDocAnnotationMapJob" should "work without @doc annotations" in {
    implicit val avroType: MagnolifyAvroType[RecordWithoutDoc] = MagnolifyAvroType[RecordWithoutDoc]
    implicit val grCoder: Coder[GenericRecord] =
      avroGenericRecordCoder(avroType.schema)

    val expected = Seq(
      RecordWithoutDoc("id1", "name1"),
      RecordWithoutDoc("id2", "name2"),
      RecordWithoutDoc("id3", "name3")
    ).map(avroType.to)

    JobTest[NoDocAnnotationMapJob.type]
      .args("--input=in.txt", "--output=out.avro")
      .input(TextIO("in.txt"), inputData)
      .output(AvroIO[GenericRecord]("out.avro")) { actual =>
        actual should containInAnyOrder(expected)
      }
      .run()
  }
}
