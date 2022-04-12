/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio.elasticsearch

import co.elastic.clients.elasticsearch.core.bulk._
import co.elastic.clients.json.JsonpSerializable
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import jakarta.json.Json
import org.scalactic.Equality
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.StringWriter

class CoderInstancesTest extends AnyFlatSpec with Matchers with CoderInstances {

  import com.spotify.scio.testing.CoderAssertions._

  type Document = Map[String, Any]
  val document = Map("key" -> 42)

  val indexOperation: IndexOperation[Document] =
    IndexOperation.of(_.index("index").id("id").document(document))
  val createOperation: CreateOperation[Document] =
    CreateOperation.of(_.index("index").id("id").document(document))
  val deleteOperation: DeleteOperation =
    DeleteOperation.of(_.index("index2").id("id2").version(2))
  val updateOperation: UpdateOperation[Document, Document] =
    UpdateOperation.of(_.index("index3").id("id3").action(_.doc(document)))

  private val mapper = new JacksonJsonpMapper()

  implicit class RichJsonpSerializable[T <: JsonpSerializable](value: T) {
    def toJson: String = {
      val writer = new StringWriter()
      value.serialize(Json.createGenerator(writer), mapper)
      writer.toString
    }
  }

  // BulkOperation doesn't have a well-defined equals method
  // compare their Json values
  implicit val indexRequestEq: Equality[BulkOperation] = {
    case (a: BulkOperation, b: BulkOperation) => a.toJson == b.toJson
    case _                                    => false
  }

  "BulkOperation coder" should "work" in {
    BulkOperation.of(_.index(indexOperation)) coderShould roundtrip()
    BulkOperation.of(_.create(createOperation)) coderShould roundtrip()
    BulkOperation.of(_.update(updateOperation)) coderShould roundtrip()
    BulkOperation.of(_.delete(deleteOperation)) coderShould roundtrip()
  }
}
