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
import com.spotify.scio.avro._
import com.spotify.scio.coders.Coder
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.{BigNumeric, Geography, Json}
import com.spotify.scio.testing._
import magnolify.scalacheck.auto._
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.joda.time.format.DateTimeFormat
import org.scalacheck._
import org.scalatest.BeforeAndAfterAll

import scala.util.{Random, Try}

object TypedBigQueryIT {
  @BigQueryType.toTable
  case class Record(
    bool: Boolean,
    int: Int,
    long: Long,
    float: Float,
    double: Double,
    numeric: BigDecimal,
    string: String,
    byteString: ByteString,
    timestamp: Instant,
    date: LocalDate,
    time: LocalTime,
    datetime: LocalDateTime,
    geography: Geography,
    json: Json,
    bigNumeric: BigNumeric
  )

  def arbBigDecimal(precision: Int, scale: Int): Arbitrary[BigDecimal] = Arbitrary {
    val max = BigInt(10).pow(precision) - 1
    Gen.choose(-max, max).map(BigDecimal(_, scale))
  }

  implicit val arbNumeric: Arbitrary[BigDecimal] =
    arbBigDecimal(Numeric.MaxNumericPrecision, Numeric.MaxNumericScale)
  implicit val arbString: Arbitrary[String] = Arbitrary(Gen.alphaStr)
  implicit val arbByteString: Arbitrary[ByteString] = Arbitrary(
    Gen.alphaStr.map(ByteString.copyFromUtf8)
  )
  // Workaround for millis rounding error
  val epochGen: Gen[Long] = Gen.chooseNum[Long](0L, 1000000000000L).map(x => x / 1000 * 1000)
  implicit val arbInstant: Arbitrary[Instant] = Arbitrary(epochGen.map(new Instant(_)))
  implicit val arbDate: Arbitrary[LocalDate] = Arbitrary(epochGen.map(new LocalDate(_)))
  implicit val arbTime: Arbitrary[LocalTime] = Arbitrary(epochGen.map(new LocalTime(_)))
  implicit val arbDatetime: Arbitrary[LocalDateTime] = Arbitrary(epochGen.map(new LocalDateTime(_)))
  implicit val arbGeography: Arbitrary[Geography] = Arbitrary(
    for {
      x <- Gen.numChar
      y <- Gen.numChar
    } yield Geography(s"POINT($x $y)")
  )
  implicit val arbJson: Arbitrary[Json] = Arbitrary(
    for {
      key <- Gen.alphaStr
      value <- Gen.alphaStr
    } yield Json(s"""{"$key":"$value"}""")
  )

  implicit val arbBigNumeric: Arbitrary[BigNumeric] = Arbitrary {
    // Precision: 76.76 (the 77th digit is partial)
    arbBigDecimal(BigNumeric.MaxNumericPrecision - 1, BigNumeric.MaxNumericScale).arbitrary
      .map(BigNumeric.apply)
  }

  private val recordGen =
    implicitly[Arbitrary[Record]].arbitrary

  private def table(name: String) = {
    val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    val now = Instant.now().toString(TIME_FORMATTER)
    val spec =
      s"data-integration-test:bigquery_avro_it.$name${now}_${Random.nextInt(Int.MaxValue)}"
    Table.Spec(spec)
  }
  private val typedTable = table("records")
  private val tableRowTable = table("records_tablerow")
  private val avroTable = table("records_avro")

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

  override protected def afterAll(): Unit = {
    val bq = BigQuery.defaultInstance()
    // best effort cleanup
    Try(bq.tables.delete(typedTable.ref))
    Try(bq.tables.delete(tableRowTable.ref))
    Try(bq.tables.delete(avroTable.ref))
  }

  "TypedBigQuery" should "handle records as TableRow" in {
    runWithRealContext(options) { sc =>
      sc.parallelize(records)
        .saveAsTypedBigQueryTable(typedTable, createDisposition = CREATE_IF_NEEDED)
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data = sc.typedBigQuery[Record](typedTable)
      data should containInAnyOrder(records)
    }
  }

  "BigQueryTypedTable" should "handle records as TableRow format" in {
    runWithRealContext(options) { sc =>
      sc.parallelize(records)
        .map(Record.toTableRow)
        .saveAsBigQueryTable(
          tableRowTable,
          schema = Record.schema,
          createDisposition = CREATE_IF_NEEDED
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data = sc.bigQueryTable(tableRowTable).map(Record.fromTableRow)
      data should containInAnyOrder(records)
    }
  }

  // TODO fix if in beam 2.61
  ignore should "handle records as avro format" in {
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(Record.avroSchema)
    runWithRealContext(options) { sc =>
      sc.parallelize(records)
        .map(Record.toAvro)
        .saveAsBigQueryTable(
          avroTable,
          schema = Record.schema, // This is a bad API. an avro schema should be expected
          createDisposition = CREATE_IF_NEEDED
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data = sc.bigQueryTable(avroTable, Format.GenericRecord).map(Record.fromAvro)
      data should containInAnyOrder(records)
    }
  }
}
