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

package com.spotify.scio.bigquery

import com.google.protobuf.ByteString
import com.spotify.scio._
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing._
import magnolify.scalacheck.auto._
import org.apache.avro.{LogicalTypes, Schema}
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.joda.time.format.DateTimeFormat
import org.scalacheck._
import org.scalatest.BeforeAndAfterAll

import scala.util.Random

object TypedBigQueryIT {
  @BigQueryType.toTable
  case class Record(
    bool: Boolean,
    int: Int,
    long: Long,
    float: Float,
    double: Double,
    string: String,
    byteString: ByteString,
    timestamp: Instant,
    date: LocalDate,
    time: LocalTime,
    datetime: LocalDateTime
  )

  // Workaround for millis rounding error
  val epochGen: Gen[Long] = Gen.chooseNum[Long](0L, 1000000000000L).map(x => x / 1000 * 1000)
  implicit val arbByteString: Arbitrary[ByteString] = Arbitrary(
    Gen.alphaStr.map(ByteString.copyFromUtf8)
  )
  implicit val arbInstant: Arbitrary[Instant] = Arbitrary(epochGen.map(new Instant(_)))
  implicit val arbDate: Arbitrary[LocalDate] = Arbitrary(epochGen.map(new LocalDate(_)))
  implicit val arbTime: Arbitrary[LocalTime] = Arbitrary(epochGen.map(new LocalTime(_)))
  implicit val arbDatetime: Arbitrary[LocalDateTime] = Arbitrary(epochGen.map(new LocalDateTime(_)))

  private val recordGen = {
    implicitly[Arbitrary[Record]].arbitrary
  }

  private def table(name: String) = {
    val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    val now = Instant.now().toString(TIME_FORMATTER)
    val spec =
      s"data-integration-test:bigquery_avro_it.$name${now}_${Random.nextInt(Int.MaxValue)}"
    Table.Spec(spec)
  }
  private val tableRowTable = table("records_tablerow")
  private val avroTable = table("records_avro")
  private val avroLogicalTypeTable = table("records_avro_logical_type")

  private val records = Gen.listOfN(100, recordGen).sample.get
  private val options = PipelineOptionsFactory
    .fromArgs(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    .create()
}

class TypedBigQueryIT extends PipelineSpec with BeforeAndAfterAll {
  import TypedBigQueryIT._

  override protected def beforeAll(): Unit = {
    val sc = ScioContext(options)
    sc.parallelize(records).saveAsTypedBigQueryTable(tableRowTable)

    sc.run()
    ()
  }

  override protected def afterAll(): Unit = {
    BigQuery.defaultInstance().tables.delete(tableRowTable.ref)
    BigQuery.defaultInstance().tables.delete(avroTable.ref)
    BigQuery.defaultInstance().tables.delete(avroLogicalTypeTable.ref)
  }

  "TypedBigQuery" should "read records" in {
    val sc = ScioContext(options)
    sc.typedBigQuery[Record](tableRowTable) should containInAnyOrder(records)
    sc.run()
  }

  it should "convert to avro format" in {
    val sc = ScioContext(options)
    implicit val coder = Coder.avroGenericRecordCoder(Record.avroSchema)
    sc.typedBigQuery[Record](tableRowTable)
      .map(Record.toAvro)
      .map(Record.fromAvro) should containInAnyOrder(
      records
    )
    sc.run()
  }

  "BigQueryTypedTable" should "read TableRow records" in {
    val sc = ScioContext(options)
    sc
      .bigQueryTable(tableRowTable)
      .map(Record.fromTableRow) should containInAnyOrder(records)
    sc.run()
  }

  it should "read GenericRecord recors" in {
    val sc = ScioContext(options)
    implicit val coder = Coder.avroGenericRecordCoder(Record.avroSchema)
    sc
      .bigQueryTable(tableRowTable, Format.GenericRecord)
      .map(Record.fromAvro) should containInAnyOrder(records)
    sc.run()
  }

  it should "write GenericRecord records" in {
    val sc = ScioContext(options)
    implicit val coder = Coder.avroGenericRecordCoder(Record.avroSchema)
    val schema =
      BigQueryUtil.parseSchema("""
        |{
        |  "fields": [
        |    {"mode": "NULLABLE", "name": "bool", "type": "BOOLEAN"},
        |    {"mode": "NULLABLE", "name": "int", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "long", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "float", "type": "FLOAT"},
        |    {"mode": "NULLABLE", "name": "double", "type": "FLOAT"},
        |    {"mode": "NULLABLE", "name": "string", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "byteString", "type": "BYTES"},
        |    {"mode": "NULLABLE", "name": "timestamp", "type": "INTEGER"},
        |    {"mode": "NULLABLE", "name": "date", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "time", "type": "STRING"},
        |    {"mode": "NULLABLE", "name": "datetime", "type": "STRING"}
        |  ]
        |}
      """.stripMargin)
    val tap = sc
      .bigQueryTable(tableRowTable, Format.GenericRecord)
      .saveAsBigQueryTable(avroTable, schema = schema, createDisposition = CREATE_IF_NEEDED)

    val result = sc.run().waitUntilDone()
    result.tap(tap).map(Record.fromAvro).value.toSet shouldBe records.toSet
  }

  it should "write GenericRecord records with logical types" in {
    val sc = ScioContext(options)
    import scala.jdk.CollectionConverters._
    val schema: Schema = Schema.createRecord(
      "Record",
      "",
      "com.spotify.scio.bigquery",
      false,
      List(
        new Schema.Field(
          "date",
          LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
          "",
          0
        ),
        new Schema.Field(
          "time",
          LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)),
          "",
          0L
        ),
        new Schema.Field("datetime", Schema.create(Schema.Type.STRING), "", "")
      ).asJava
    )
    implicit val coder = Coder.avroGenericRecordCoder(schema)
    val ltRecords: Seq[GenericRecord] =
      Seq(
        new GenericRecordBuilder(schema)
          .set("date", 10)
          .set("time", 1000L)
          .set("datetime", "2020-08-03 11:11:11")
          .build()
      )

    val tableSchema =
      BigQueryUtil.parseSchema("""
        |{
        |  "fields": [
        |    {"mode": "REQUIRED", "name": "date", "type": "DATE"},
        |    {"mode": "REQUIRED", "name": "time", "type": "TIME"},
        |    {"mode": "REQUIRED", "name": "datetime", "type": "STRING"}
        |  ]
        |}
      """.stripMargin)
    val tap = sc
      .parallelize(ltRecords)
      .saveAsBigQueryTable(avroLogicalTypeTable, tableSchema, createDisposition = CREATE_IF_NEEDED)

    val result = sc.run().waitUntilDone()
    result.tap(tap).value.toList.size shouldBe 1
  }

}
