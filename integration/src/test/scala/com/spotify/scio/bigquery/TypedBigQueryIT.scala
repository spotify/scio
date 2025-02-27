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
import com.spotify.scio.bigquery.BigQueryTypedTable.Format.GenericRecordWithLogicalTypes
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.{BigNumeric, Geography, Json}
import com.spotify.scio.testing._
import magnolify.scalacheck.auto._
import org.apache.avro.UnresolvedUnionException
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{Method => WriteMethod}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.joda.time.format.DateTimeFormat
import org.scalacheck._
import org.scalatest.BeforeAndAfterAll

import scala.util.{Random, Try}

object TypedBigQueryIT {
  case class Nested(int: Int)

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
    // BQ DATETIME is problematic with avro as BQ api uses different representations:
    // - BQ export uses 'string(datetime)'
    // - BQ load uses 'long(local-timestamp-micros)'
    // BigQueryType avroSchema favors read with string type
    // datetime: LocalDateTime,
    geography: Geography,
    json: Json,
    bigNumeric: BigNumeric,
    nestedRequired: Nested,
    nestedOptional: Option[Nested]
  )

  // A record with no nested record types
  @BigQueryType.toTable
  case class FlatRecord(
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
  implicit val arbJson: Arbitrary[Json] = Arbitrary {
    import Arbitrary._
    import Gen._
    Gen
      .oneOf(
        // json object
        alphaLowerStr.flatMap(str => arbInt.arbitrary.map(num => s"""{"$str":$num}""")),
        // json array
        alphaLowerStr.flatMap(str => arbInt.arbitrary.map(num => s"""["$str",$num]""")),
        // json literals
        alphaLowerStr.map(str => s""""$str""""),
        arbInt.arbitrary.map(_.toString),
        arbBool.arbitrary.map(_.toString),
        Gen.const("null")
      )
      .map(wkt => Json(wkt))
  }

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
  private val typedTableFileLoads = table("records_fileloads")
  private val typedTableStorage = table("records_storage")
  private val tableRowTable = table("records_tablerow")
  private val avroTable = table("records_avro")
  private val tableRowStorage = table("records_tablerow_storage")
  private val avroFlatTable = table("records_avro_flat")

  private val records = Gen.listOfN(5, recordGen).sample.get
  private val options = PipelineOptionsFactory
    .fromArgs(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-eu/temp"
    )
    .create()
}

class TypedBigQueryIT extends PipelineSpec with BeforeAndAfterAll {
  import TypedBigQueryIT._

  private val bq = BigQuery.defaultInstance()

  override protected def afterAll(): Unit = {
    // best effort cleanup
    Try(bq.tables.delete(typedTableFileLoads.ref))
    Try(bq.tables.delete(typedTableStorage.ref))
    Try(bq.tables.delete(tableRowTable.ref))
    Try(bq.tables.delete(avroTable.ref))
    Try(bq.tables.delete(tableRowStorage.ref))
    Try(bq.tables.delete(avroFlatTable.ref))
  }

  "TypedBigQuery" should "write case classes using FileLoads API" in {
    runWithRealContext(options) { sc =>
      sc.parallelize(records)
        .saveAsTypedBigQueryTable(
          typedTableFileLoads,
          createDisposition = CREATE_IF_NEEDED,
          method = WriteMethod.FILE_LOADS
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data = sc.typedBigQuery[Record](typedTableFileLoads)
      data should containInAnyOrder(records)
    }
  }

  it should "write case classes using Storage Write API" in {
    // Storage write API has a bug impacting TIME field writes: https://github.com/apache/beam/issues/34038
    // Todo remove when fixed
    the[IllegalArgumentException] thrownBy {
      runWithRealContext(options) { sc =>
        sc.parallelize(records)
          .saveAsTypedBigQueryTable(
            typedTableStorage,
            createDisposition = CREATE_IF_NEEDED,
            method = WriteMethod.STORAGE_WRITE_API
          )
      }.waitUntilFinish()

      runWithRealContext(options) { sc =>
        val data = sc.typedBigQuery[Record](typedTableStorage)
        data should containInAnyOrder(records)
      }
    } should have message "TIME schemas are not currently supported for Typed Storage Write API writes. Please use Write method FILE_LOADS instead, or map case classes using BigQueryType.toTableRow and use saveAsBigQueryTable directly."
  }

  it should "write case classes manually converted to TableRows using FileLoads API" in {
    runWithRealContext(options) { sc =>
      sc.parallelize(records)
        .map(Record.toTableRow)
        .map { row =>
          // TableRow BQ save API uses json
          // TO disambiguate from literal json string,
          // field MUST be converted to parsed JSON
          val jsonLoadRow = new TableRow()
          jsonLoadRow.putAll(row.asInstanceOf[java.util.Map[String, _]]) // cast for 2.12
          jsonLoadRow.set("json", Json.parse(row.getJson("json")))
        }
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

  it should "write case classes manually converted to GenericRecords using FileLoads API" in {
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
      sc.typedBigQuery[Record](avroTable) should containInAnyOrder(records)
    }

    // Due to Beam bug with automatic schema detection, can't parse nested record types as GenericRecords yet
    // Todo remove assertThrows after fixing in Beam
    assertThrows[UnresolvedUnionException] {
      runWithRealContext(options) { sc =>
        val data =
          sc.bigQueryTable(avroTable, format = GenericRecordWithLogicalTypes)
            .map(Record.fromAvro)
        data should containInAnyOrder(records)
      }
    }
  }

  it should "write case classes manually converted to TableRows using Storage Write API" in {
    runWithRealContext(options) { sc =>
      sc.parallelize(records)
        .map(Record.toTableRow)
        .saveAsBigQueryTable(
          tableRowStorage,
          schema = Record.schema,
          createDisposition = CREATE_IF_NEEDED,
          method = WriteMethod.STORAGE_WRITE_API
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      sc.typedBigQuery[Record](tableRowStorage) should containInAnyOrder(records)
    }
  }

  it should "read BigQuery rows into GenericRecords and convert them to case classes for records without nested types" in {
    implicit val coder: Coder[GenericRecord] = avroGenericRecordCoder(FlatRecord.avroSchema)

    val flatRecords = Gen.listOfN(5, implicitly[Arbitrary[FlatRecord]].arbitrary).sample.get

    runWithRealContext(options) { sc =>
      sc.parallelize(flatRecords)
        .map(FlatRecord.toAvro)
        .saveAsBigQueryTable(
          avroFlatTable,
          schema = FlatRecord.schema,
          createDisposition = CREATE_IF_NEEDED
        )
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data =
        sc.bigQueryTable(avroFlatTable, Format.GenericRecordWithLogicalTypes)
          .map(FlatRecord.fromAvro)
      data should containInAnyOrder(flatRecords)
    }
  }
}
