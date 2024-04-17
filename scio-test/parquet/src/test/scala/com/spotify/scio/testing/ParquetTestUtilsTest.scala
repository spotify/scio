/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.testing

import com.spotify.scio.avro.TestRecord
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.parquet.filter2.predicate.FilterApi
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

case class SomeRecord(intField: Int)

class ParquetTestUtilsTest extends AnyFlatSpec with Matchers with ParquetTestUtils {

  "Avro SpecificRecords" should "be filterable and projectable" in {
    val records = (1 to 10).map(i =>
      new TestRecord(
        i,
        i.toLong,
        i.toFloat,
        i.toDouble,
        true,
        "hello",
        List[CharSequence]("a", "b", "c").asJava
      )
    )

    val transformed = records
      .parquetFilter(
        FilterApi.gt(FilterApi.intColumn("int_field"), 5.asInstanceOf[java.lang.Integer])
      )
      .parquetProject(
        SchemaBuilder.record("TestRecord").fields().optionalInt("int_field").endRecord()
      )

    transformed.map(_.int_field) should contain theSameElementsAs Seq(6, 7, 8, 9, 10)
    transformed.foreach { r =>
      r.long_field shouldBe null
      r.string_field shouldBe null
      r.double_field shouldBe null
      r.boolean_field shouldBe null
    }
  }

  "Avro GenericRecords" should "be filterable and projectable" in {
    val recordSchema = SchemaBuilder
      .record("TestRecord")
      .fields()
      .requiredInt("int_field")
      .optionalString("string_field")
      .endRecord()

    val records = (1 to 10).map(i =>
      new GenericRecordBuilder(recordSchema)
        .set("int_field", i)
        .set("string_field", i.toString)
        .build()
    )

    val transformed = records
      .parquetFilter(
        FilterApi.gt(FilterApi.intColumn("int_field"), Int.box(5))
      )
      .parquetProject(
        SchemaBuilder.record("Projection").fields().optionalInt("int_field").endRecord()
      )

    transformed.map(_.get("int_field").toString.toInt) should contain theSameElementsAs Seq(6, 7, 8,
      9, 10)
    transformed.foreach { r =>
      r.get("string_field") shouldBe null
    }
  }

  "Case classes" should "be filterable" in {
    val records = (1 to 10).map(SomeRecord)

    val transformed = records
      .parquetFilter(FilterApi.gt(FilterApi.intColumn("intField"), Int.box(5)))

    transformed.map(_.intField) should contain theSameElementsAs Seq(6, 7, 8, 9, 10)
  }
}
