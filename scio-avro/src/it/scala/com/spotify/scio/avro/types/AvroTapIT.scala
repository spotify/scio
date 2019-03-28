/*
 * Copyright 2019 Spotify AB.
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

import com.spotify.scio.avro.AvroTaps
import com.spotify.scio.io.Taps
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

final class AvroTapIT extends FlatSpec with Matchers {
  private val schema = new Parser().parse("""{
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

  it should "read avro file" in {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create)

    val asd = AvroTaps(Taps()).avroFile[GenericRecord](
      "gs://data-integration-test-eu/avro-integration-test/folder-a/folder-b/shakespeare.avro",
      schema = schema)
    val result = Await.result(asd, Duration.Inf)

    result.value.hasNext shouldBe true
  }

}
