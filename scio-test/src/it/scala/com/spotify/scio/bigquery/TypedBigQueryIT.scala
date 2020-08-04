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
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.testing._
import magnolify.scalacheck.auto._
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.scalacheck._
import org.scalatest.BeforeAndAfterAll

import scala.util.Random
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.coders.Coder

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
  private val records = Gen.listOfN(1, recordGen).sample.get
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
    result.tap(tap).map(Record.fromAvro).value.toList shouldBe records
  }
}
