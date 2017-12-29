/*
 * Copyright 2017 Spotify AB.
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
import com.spotify.scio.testing._
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.format.DateTimeFormat
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.scalacheck._
import org.scalacheck.ScalacheckShapeless._
import org.scalatest._

import scala.util.Random

object TypedBigQueryIT {
  @BigQueryType.toTable
  case class Record(bool: Boolean, int: Int, long: Long, float: Float, double: Double,
                    string: String, byteString: ByteString,
                    timestamp: Instant, date: LocalDate, time: LocalTime, datetime: LocalDateTime)

  // Workaround for millis rounding error
  val epochGen = Gen.chooseNum[Long](0L, 1000000000000L).map(x => x / 1000 * 1000)
  implicit val arbByteString = Arbitrary(Gen.alphaStr.map(ByteString.copyFromUtf8))
  implicit val arbInstant = Arbitrary(epochGen.map(new Instant(_)))
  implicit val arbDate = Arbitrary(epochGen.map(new LocalDate(_)))
  implicit val arbTime = Arbitrary(epochGen.map(new LocalTime(_)))
  implicit val arbDatetime = Arbitrary(epochGen.map(new LocalDateTime(_)))

  private val recordGen = {
    implicitly[Arbitrary[Record]].arbitrary
  }
}

class TypedBigQueryIT extends PipelineSpec with BeforeAndAfterAll {

  import TypedBigQueryIT._

  private val table = {
    val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    val now = Instant.now().toString(TIME_FORMATTER)
    "data-integration-test:bigquery_avro_it.records_" + now + "_" + Random.nextInt(Int.MaxValue)
  }
  private val records = Gen.listOfN(1000, recordGen).sample.get

  override protected def beforeAll(): Unit = {
    val bq = BigQueryClient.defaultInstance()
    bq.writeTypedRows[Record](table, records)
  }

  private val options = PipelineOptionsFactory
    .fromArgs(
      "--project=data-integration-test",
      "--tempLocation=gs://data-integration-test-us/temp")
    .create()

  "TypedBigQuery" should "work" in {
    val sc = ScioContext(options)
    sc.typedBigQuery[Record](table) should containInAnyOrder (records)
    sc.close()
  }

}
