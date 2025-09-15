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

package com.spotify.scio.testing.parquet

import com.spotify.scio.avro.{Account, AccountStatus}
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder
import org.apache.parquet.filter2.predicate.FilterApi
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.tensorflow.metadata.{v0 => tfmd}
import org.tensorflow.proto.example._

import scala.jdk.CollectionConverters._

case class SomeRecord(id: Int)

class ParquetTestUtilsTest extends AnyFlatSpec with Matchers {

  "Avro SpecificRecords" should "be filterable and projectable via Schema" in {
    import com.spotify.scio.testing.parquet.avro._

    val records = (1 to 10).map(i =>
      Account
        .newBuilder()
        .setId(i)
        .setName(i.toString)
        .setAmount(i.toDouble)
        .setType(s"Type$i")
        .setAccountStatus(AccountStatus.Active)
        .build()
    )

    val filter = FilterApi.gt(FilterApi.intColumn("id"), 5.asInstanceOf[java.lang.Integer])
    val projection = SchemaBuilder.record("Account").fields().requiredInt("id").endRecord()

    val transformed = records withFilter filter withProjection projection
    transformed.map(_.getId) should contain theSameElementsAs Seq(6, 7, 8, 9, 10)
    transformed.foreach { r =>
      r.getName shouldBe null
      r.getAmount shouldBe 0.0d
      r.getAccountStatus shouldBe null
      r.getType shouldBe null
    }
  }

  it should "be projectable via case class" in {
    import com.spotify.scio.testing.parquet.avro._

    val records = (1 to 10).map(i =>
      Account
        .newBuilder()
        .setId(i)
        .setName(i.toString)
        .setAmount(i.toDouble)
        .setType(s"Type$i")
        .setAccountStatus(AccountStatus.Active)
        .build()
    )

    records.withProjection[SomeRecord] should contain theSameElementsAs (1 to 10).map(SomeRecord)
  }

  "Avro GenericRecords" should "be filterable and projectable via Schema" in {
    import com.spotify.scio.testing.parquet.avro._

    val recordSchema = SchemaBuilder
      .record("TestRecord")
      .fields()
      .requiredInt("id")
      .optionalString("string_field")
      .endRecord()

    val records = (1 to 10).map(i =>
      new GenericRecordBuilder(recordSchema)
        .set("id", i)
        .set("string_field", i.toString)
        .build()
    )

    val filter = FilterApi.gt(FilterApi.intColumn("id"), 5.asInstanceOf[java.lang.Integer])
    val projection =
      SchemaBuilder.record("Projection").fields().optionalInt("id").endRecord()

    records withFilter filter withProjection projection should contain theSameElementsAs Seq(6, 7,
      8, 9, 10).map(i =>
      new GenericRecordBuilder(recordSchema)
        .set("id", i)
        .set("string_field", null)
        .build()
    )
  }

  it should "be projectable via case class" in {
    import com.spotify.scio.testing.parquet.avro._

    val recordSchema = SchemaBuilder
      .record("TestRecord")
      .fields()
      .requiredInt("id")
      .optionalString("string_field")
      .endRecord()

    val records = (1 to 10).map(i =>
      new GenericRecordBuilder(recordSchema)
        .set("id", i)
        .set("string_field", i.toString)
        .build()
    )

    records.withProjection[SomeRecord] should contain theSameElementsAs (1 to 10).map(SomeRecord)
  }

  "Case classes" should "be filterable" in {
    import com.spotify.scio.testing.parquet.types._

    val records = (1 to 10).map(SomeRecord)

    records withFilter (
      FilterApi.gt(FilterApi.intColumn("id"), Int.box(5))
    ) should contain theSameElementsAs Seq(6, 7, 8, 9, 10).map(SomeRecord)
  }

  "TfExamples" should "be filterable and projectable" in {
    import com.spotify.scio.testing.parquet.tensorflow._

    val required = tfmd.ValueCount.newBuilder().setMin(1).setMax(1).build()

    val schema = tfmd.Schema
      .newBuilder()
      .addFeature(
        tfmd.Feature
          .newBuilder()
          .setName("int64_required")
          .setType(tfmd.FeatureType.INT)
          .setValueCount(required)
          .build()
      )
      .addFeature(
        tfmd.Feature
          .newBuilder()
          .setName("float_required")
          .setType(tfmd.FeatureType.FLOAT)
          .setValueCount(required)
          .build()
      )
      .build()

    val records = (1 to 10).map(i =>
      Example
        .newBuilder()
        .setFeatures(
          Features
            .newBuilder()
            .putFeature(
              "int64_required",
              Feature
                .newBuilder()
                .setInt64List(
                  Int64List
                    .newBuilder()
                    .addAllValue(Seq(i.toLong).asInstanceOf[Seq[java.lang.Long]].asJava)
                )
                .build()
            )
            .putFeature(
              "float_required",
              Feature
                .newBuilder()
                .setFloatList(
                  FloatList
                    .newBuilder()
                    .addAllValue(Seq(10 - i.toFloat).asInstanceOf[Seq[java.lang.Float]].asJava)
                )
                .build()
            )
            .build()
        )
        .build()
    )

    records withFilter (
      schema,
      FilterApi.gt(FilterApi.floatColumn("float_required"), Float.box(5.5f))
    ) withProjection (
      schema,
      tfmd.Schema
        .newBuilder()
        .addFeature(
          tfmd.Feature
            .newBuilder()
            .setName("int64_required")
            .setType(tfmd.FeatureType.INT)
            .setValueCount(required)
            .build()
        )
        .build()
    ) should contain theSameElementsAs (1 to 4).map(i =>
      Example
        .newBuilder()
        .setFeatures(
          Features
            .newBuilder()
            .putFeature(
              "int64_required",
              Feature
                .newBuilder()
                .setInt64List(
                  Int64List
                    .newBuilder()
                    .addAllValue(Seq(i.toLong).asInstanceOf[Seq[java.lang.Long]].asJava)
                )
                .build()
            )
            .build()
        )
        .build()
    )
  }
}
