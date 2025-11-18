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

package com.spotify.scio.bigquery

import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.coders.Coder
import com.spotify.scio.testing._
import magnolify.bigquery._
import magnolify.bigquery.unsafe._
import magnolify.scalacheck.auto._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{Method => WriteMethod}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.scalacheck._
import org.scalatest.BeforeAndAfterAll

import java.time.format.DateTimeFormatter
import java.time._

object MagnolifyBigQueryIT {
  case class Nested(int: Int)

  case class Record(
    bool: Boolean,
    long: Long,
    double: Double,
    numeric: BigDecimal,
    string: String,
    timestamp: Instant,
    date: LocalDate,
    time: LocalTime,
    datetime: LocalDateTime,
    nestedRequired: Nested,
    nestedOptional: Option[Nested]
  )

  def arbBigDecimal(precision: Int, scale: Int): Arbitrary[BigDecimal] = Arbitrary {
    val max = BigInt(10).pow(precision) - 1
    Gen.choose(-max, max).map(BigDecimal(_, scale))
  }

  implicit val arbNumeric: Arbitrary[BigDecimal] =
    arbBigDecimal(Numeric.MaxNumericPrecision, Numeric.MaxNumericScale)
  implicit val arbString: Arbitrary[String] = Arbitrary(Gen.alphaStr)

  // Workaround for millis rounding error
  val genInstant: Gen[Instant] =
    Gen.chooseNum[Long](0L, 1000000000000L).map(x => x / 1000 * 1000).map(Instant.ofEpochMilli)
  implicit val arbInstant: Arbitrary[Instant] = Arbitrary(genInstant)
  implicit val arbDate: Arbitrary[LocalDate] = Arbitrary(
    genInstant.map(_.atZone(ZoneOffset.UTC).toLocalDate)
  )
  implicit val arbTime: Arbitrary[LocalTime] = Arbitrary(
    genInstant.map(_.atZone(ZoneOffset.UTC).toLocalTime)
  )
  implicit val arbDatetime: Arbitrary[LocalDateTime] = Arbitrary(
    genInstant.map(_.atZone(ZoneOffset.UTC).toLocalDateTime)
  )

  private def table(name: String) = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
    val now = Instant.now().atZone(ZoneOffset.UTC).toLocalDateTime.format(formatter)
    val spec =
      s"data-integration-test:bigquery_avro_it.magnolify_$name${now}"
    Table.Spec(spec)
  }

  def sample[T](gen: Gen[T]): Seq[T] = Gen.listOfN(5, gen).sample.get

  val records = sample(implicitly[Arbitrary[Record]].arbitrary)

  private val options = PipelineOptionsFactory
    .fromArgs(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    .create()
}

class MagnolifyBigQueryIT extends PipelineSpec with BeforeAndAfterAll {
  import MagnolifyBigQueryIT._

  private val bq = BigQuery.defaultInstance()

  override protected def afterAll(): Unit =
    bq.client
      .execute(
        _.tables().list("data-integration-test", "bigquery_avro_it")
      )
      .getTables
      .forEach(t => bq.tables.delete(t.getTableReference))

  def testRoundtrip[T: Coder: TableRowType](
    writeMethod: WriteMethod,
    readStorageApi: Boolean
  )(rows: Seq[T]): Unit = {
    val trt = implicitly[TableRowType[T]]
    val tableRef = table(s"${writeMethod}_storageAPI_${readStorageApi}".toLowerCase)

    runWithRealContext(options) { sc =>
      sc
        .parallelize(rows)
        .map(trt.to)
        .saveAsBigQueryTable(
          tableRef,
          schema = trt.schema,
          createDisposition = CREATE_IF_NEEDED,
          method = writeMethod
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data = if (readStorageApi) {
        sc
          .bigQueryStorage(tableRef)
          .map(trt.from)
      } else {
        sc
          .bigQueryTable(tableRef, Format.TableRow)
          .map(trt.from)
      }
      data should containInAnyOrder(rows)
    }
  }

  "MagnolifyBigQuery" should "write case classes using FileLoads API and read using Extract API" in {
    testRoundtrip(WriteMethod.FILE_LOADS, readStorageApi = false)(records)
  }

  it should "write case classes using FileLoads API and read using Storage API" in {
    testRoundtrip(WriteMethod.FILE_LOADS, readStorageApi = true)(records)
  }

  it should "read case classes written using legacy BigQueryType" in {
    val tableRef = table("magnolify_bqt_compat")
    val records: Seq[TypedBigQueryIT.Record] = TypedBigQueryIT.records

    runWithRealContext(options) { sc =>
      sc
        .parallelize(records)
        .saveAsTypedBigQueryTable(
          tableRef,
          createDisposition = CREATE_IF_NEEDED
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      import org.joda.time.{DateTimeFieldType, DateTimeZone}

      val trt = TableRowType[Record]
      val data = sc
        .bigQueryStorage(tableRef)
        .map(trt.from)

      // We're comparing two different Record classes; convert fields to comparable types
      data.map(r =>
        (
          r.bool,
          r.long,
          r.double,
          r.numeric,
          r.string,
          r.timestamp.toEpochMilli,
          r.date.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli,
          r.time.toSecondOfDay,
          r.datetime.toInstant(ZoneOffset.UTC).toEpochMilli,
          r.nestedRequired.int,
          r.nestedOptional.map(_.int)
        )
      ) should containInAnyOrder(
        records.map(r =>
          (
            r.bool,
            r.long,
            r.double,
            r.numeric,
            r.string,
            r.timestamp.toInstant.getMillis,
            r.date.toDateTimeAtStartOfDay(DateTimeZone.UTC).getMillis,
            r.time.get(DateTimeFieldType.secondOfDay()),
            r.datetime.toDateTime(DateTimeZone.UTC).getMillis,
            r.nestedRequired.int,
            r.nestedOptional.map(_.int)
          )
        )
      )
    }
  }
}
