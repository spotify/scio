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

package com.spotify.scio.avro.types

import org.apache.avro.Schema.Parser
import org.scalatest.{FlatSpec, Matchers}

object AvroTypeIT {
  @AvroType.fromPath(
    "gs://data-integration-test-eu/avro-integration-test/folder-a/folder-b/")
  class FromPath

  @AvroType.fromPath(
    "gs://data-integration-test-eu/*/*/*/")
  class FromGlob

  @AvroType.fromPath(
    """
      |gs://data-integration-test-eu/
      |avro-integration-test/folder-a/folder-b/
    """.stripMargin)
  class FromPathMultiLine
}

class AvroTypeIT extends FlatSpec with Matchers  {
  import AvroTypeIT._

  private val expectedSchema = new Parser().parse("""{
                                                  |  "type" : "record",
                                                  |  "name" : "Root",
                                                  |  "fields" : [ {
                                                  |    "name" : "word",
                                                  |    "type" : [ "string", "null" ]
                                                  |  }, {
                                                  |    "name" : "word_count",
                                                  |    "type" : [ "long", "null" ]
                                                  |  }, {
                                                  |    "name" : "corpus",
                                                  |    "type" : [ "string", "null" ]
                                                  |  }, {
                                                  |    "name" : "corpus_date",
                                                  |    "type" : [ "long", "null" ]
                                                  |  } ]
                                                  |}""".stripMargin)

  "fromPath" should "correctly read schema from GCS path" in {
    FromPath.schema shouldBe expectedSchema
  }

  it should "correctly read schema from GCS glob" in {
    FromGlob.schema shouldBe expectedSchema
  }

  it should "correctly read schema from multilne GCS path" in {
    FromPathMultiLine.schema shouldBe expectedSchema
  }

  it should "support roundtrip conversion when reading schema from GCS path" in {
    val r1 = FromPath(Some("word"), Some(2L), Some("corpus"), Some(123L))
    val r2 = FromPath.fromGenericRecord(FromPath.toGenericRecord(r1))
    r1 shouldBe r2
  }

  it should "support roundtrip conversion when reading schema from GCS glob" in {
    val r1 = FromGlob(word=Some("word"), word_count=Some(2L))
    val r2 = FromGlob.fromGenericRecord(FromGlob.toGenericRecord(r1))
    r1 shouldBe r2
  }

  it should "support roundtrip conversion when reading schema from multilne GCS path" in {
    val r1 = FromPathMultiLine(corpus=Some("corpus"), corpus_date=Some(123L))
    val r2 = FromPathMultiLine.fromGenericRecord(FromPathMultiLine.toGenericRecord(r1))
    r1 shouldBe r2
  }
}
