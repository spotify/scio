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
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.BigQueryTypedTable.Format
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.types.{BigNumeric, Geography, Json}
import com.spotify.scio.testing._
import magnolify.scalacheck.auto._
import org.apache.avro.UnresolvedUnionException
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{Method => WriteMethod}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.joda.time.format.DateTimeFormat
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.scalacheck._
import org.scalatest.BeforeAndAfterAll

import scala.util.Random
import scala.reflect.runtime.universe._

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
    datetime: LocalDateTime,
    geography: Geography,
    json: Json,
    bigNumeric: BigNumeric,
    nestedRequired: Nested,
    nestedOptional: Option[Nested]
  )

  @BigQueryType.toTable
  case class RecordNoJson(
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
    bigNumeric: BigNumeric,
    nestedRequired: Nested,
    nestedOptional: Option[Nested]
  )

  @BigQueryType.toTable
  case class RecordNoTime(
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
    datetime: LocalDateTime,
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

  private def table(name: String) = {
    val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    val now = Instant.now().toString(TIME_FORMATTER)
    val spec =
      s"data-integration-test:bigquery_avro_it.$name${now}_${Random.nextInt(Int.MaxValue)}"
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

class TypedBigQueryIT extends PipelineSpec with BeforeAndAfterAll {
  import TypedBigQueryIT._

  private val bq = BigQuery.defaultInstance()

  override protected def afterAll(): Unit =
    bq.client
      .execute(
        _.tables().list("data-integration-test", "bigquery_avro_it")
      )
      .getTables
      .forEach(t => bq.tables.delete(t.getTableReference))

  def testRoundtrip[T <: HasAnnotation: TypeTag: Coder, WF, RF](
    writeFormat: Format[WF],
    writeMethod: WriteMethod,
    readFormat: Option[Format[RF]] = None
  )(rows: Seq[T]): Unit = {
    val tableRef = table(s"${writeFormat}_${writeMethod}".toLowerCase)
    lazy val bqt = BigQueryType[T]

    implicit val grCoder: Coder[GenericRecord] = avroGenericRecordCoder(bqt.avroSchema)

    runWithRealContext(options) { sc =>
      writeFormat match {
        case Format.TableRow =>
          sc
            .parallelize(rows)
            .map(bqt.toTableRow)
            .map { row =>
              if (BigQueryUtil.isStorageApiWrite(writeMethod) || !row.containsKey("json")) {
                row
              } else {
                // TableRow BQ save API uses json
                // TO disambiguate from literal json string,
                // field MUST be converted to parsed JSON
                val jsonLoadRow = new TableRow()
                jsonLoadRow.putAll(row.asInstanceOf[java.util.Map[String, _]]) // cast for 2.12
                jsonLoadRow.set("json", Json.parse(row.getJson("json")))
              }
            }
            .saveAsBigQueryTable(
              tableRef,
              schema = bqt.schema,
              createDisposition = CREATE_IF_NEEDED,
              method = writeMethod
            )
        case Format.GenericRecordWithLogicalTypes | Format.GenericRecord =>
          // GenericRecord is the default repr
          sc.parallelize(rows)
            .saveAsTypedBigQueryTable(
              tableRef,
              createDisposition = CREATE_IF_NEEDED,
              method = writeMethod
            )
      }
    }.waitUntilFinish()

    runWithRealContext(options) { sc =>
      val data = readFormat match {
        case Some(Format.TableRow) =>
          sc
            .bigQueryTable(tableRef, Format.TableRow)
            .map(bqt.fromTableRow)
        case Some(Format.GenericRecord) | Some(Format.GenericRecordWithLogicalTypes) =>
          sc
            .bigQueryTable(tableRef, Format.GenericRecordWithLogicalTypes)
            .map(bqt.fromAvro)
        case None =>
          sc
            .typedBigQuery[T](tableRef)
      }
      data should containInAnyOrder(rows)
    }
  }

  "TypedBigQuery" should "write case classes using TableRow representation and FileLoads API" in {
    testRoundtrip(Format.TableRow, WriteMethod.FILE_LOADS)(records)
  }

  // Beam's bq-to-Avro conversion format works for StorageWrites API, but not FileLoads API;
  // Scio attempts to provide a workaround for most schema types
  it should "write case classes with a LocalDateTime field using GenericRecord representation and FileLoads API" in {
    testRoundtrip(Format.GenericRecordWithLogicalTypes, WriteMethod.FILE_LOADS)(
      sample(implicitly[Arbitrary[RecordNoJson]].arbitrary)
    )
  }

  // Workaround fails if record contains a JSON field; see: https://github.com/spotify/scio/pull/5623
  it should "fail to write case classes with a LocalDateTime field using GenericRecord representation and FileLoads API " +
    "if the record also contains a json fieled" in {
      val error = intercept[PipelineExecutionException] {
        testRoundtrip(Format.GenericRecordWithLogicalTypes, WriteMethod.FILE_LOADS)(records)
      }

      error.getMessage should include(
        "Field datetime has incompatible types. Configured schema: datetime; Avro file: string."
      )
    }

  it should "write case classes using TableRow representation and Storage API" in {
    testRoundtrip(Format.TableRow, WriteMethod.STORAGE_WRITE_API)(records)
  }

  // Storage write API has a bug impacting TIME field writes w/ GenericRecord format:
  // https://github.com/apache/beam/issues/34038
  // Todo remove special casing when fixed
  it should "write case classes without LocalTime or LocalDateTime fields using GenericRecord representation and Storage API" in {
    testRoundtrip(Format.GenericRecordWithLogicalTypes, WriteMethod.STORAGE_WRITE_API)(
      sample(implicitly[Arbitrary[RecordNoTime]].arbitrary)
    )
  }

  it should "fail when writing case classes with LocalTime fields using GenericRecord representation and Storage API" in {
    the[IllegalArgumentException] thrownBy {
      testRoundtrip(Format.GenericRecordWithLogicalTypes, WriteMethod.STORAGE_WRITE_API)(records)
    } should have message "TIME schemas are not currently supported for Typed Storage Write API writes. Please use Write method FILE_LOADS instead, or map case classes using BigQueryType.toTableRow and use saveAsBigQueryTable directly."
  }

  it should "read rows in TableRow format and manually convert to case classes" in {
    testRoundtrip(
      Format.GenericRecordWithLogicalTypes,
      WriteMethod.STORAGE_WRITE_API,
      Some(Format.TableRow)
    )(sample(implicitly[Arbitrary[RecordNoTime]].arbitrary))
  }

  it should "read rows without nested record fields in GenericRecord format" in {
    testRoundtrip(
      Format.TableRow,
      WriteMethod.STORAGE_WRITE_API,
      Some(Format.GenericRecordWithLogicalTypes)
    )(sample(implicitly[Arbitrary[FlatRecord]].arbitrary))
  }

  it should "fail to read rows with nested record fields in GenericRecord format" in {
    // Due to Beam bug with automatic schema detection, can't parse nested record types as GenericRecords yet
    // Todo remove assertThrows after fixing in Beam
    assertThrows[UnresolvedUnionException] {
      testRoundtrip(
        Format.TableRow,
        WriteMethod.STORAGE_WRITE_API,
        Some(Format.GenericRecordWithLogicalTypes)
      )(records)
    }
  }
}
