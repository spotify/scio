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

package com.spotify.scio.bigquery.types

import com.google.protobuf.ByteString
import com.spotify.scio.bigquery.client.BigQuery
import org.joda.time.{DateTimeZone, Duration, Instant, LocalDate, LocalDateTime, LocalTime}

// Run this to re-populate tables for integration tests
object BigQueryITUtil {

  @BigQueryType.toTable
  case class ToTableT(word: String, word_count: Int)

  @BigQueryType.toTable
  case class Required(bool: Boolean,
                      int: Long,
                      float: Double,
                      numeric: BigDecimal,
                      string: String,
                      bytes: ByteString,
                      timestamp: Instant,
                      date: LocalDate,
                      time: LocalTime,
                      datetime: LocalDateTime)

  @BigQueryType.toTable
  case class Optional(bool: Option[Boolean],
                      int: Option[Long],
                      float: Option[Double],
                      numeric: Option[BigDecimal],
                      string: Option[String],
                      bytes: Option[ByteString],
                      timestamp: Option[Instant],
                      date: Option[LocalDate],
                      time: Option[LocalTime],
                      datetime: Option[LocalDateTime])

  @BigQueryType.toTable
  case class Repeated(bool: List[Boolean],
                      int: List[Long],
                      float: List[Double],
                      numeric: List[BigDecimal],
                      string: List[String],
                      bytes: List[ByteString],
                      timestamp: List[Instant],
                      date: List[LocalDate],
                      time: List[LocalTime],
                      datetime: List[LocalDateTime])

  case class Record(int: Long, string: String)

  @BigQueryType.toTable
  case class Nested(required: Record, optional: Option[Record], repeated: List[Record])

  def main(args: Array[String]): Unit = {
    val bq = BigQuery.defaultInstance()

    populatePartitionedTables(bq)
    populateStorageTables(bq)
  }

  private def populatePartitionedTables(bq: BigQuery): Unit = {
    val data = List(ToTableT("a", 1), ToTableT("b", 2))
    bq.writeTypedRows("data-integration-test:partition_a.table_20170101", data)
    bq.writeTypedRows("data-integration-test:partition_a.table_20170102", data)
    bq.writeTypedRows("data-integration-test:partition_a.table_20170103", data)
    bq.writeTypedRows("data-integration-test:partition_b.table_20170101", data)
    bq.writeTypedRows("data-integration-test:partition_b.table_20170102", data)
    bq.writeTypedRows("data-integration-test:partition_c.table_20170104", data)
    ()
  }

  private def populateStorageTables(bq: BigQuery): Unit = {
    val required = (0 until 10).toList.map(newRequired)
    val optional = (0 until 10).toList.map(newOptional)
    val repeated = (0 until 10).toList.map(newRepeated)
    val nested = (0 until 10).toList.map { i =>
      val r = Record(i, s"s$i")
      Nested(r, Some(r), List(r))
    }

    bq.writeTypedRows("data-integration-test:storage.required", required)
    bq.writeTypedRows("data-integration-test:storage.optional", optional)
    bq.writeTypedRows("data-integration-test:storage.repeated", repeated)
    bq.writeTypedRows("data-integration-test:storage.nested", nested)
    ()
  }

  private def newRequired(i: Int): Required = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Required(
      true,
      i,
      i,
      BigDecimal(i),
      s"s$i",
      ByteString.copyFromUtf8(s"s$i"),
      t.plus(Duration.millis(i)),
      dt.toLocalDate.plusDays(i),
      dt.toLocalTime.plusMillis(i),
      dt.toLocalDateTime.plusMillis(i)
    )
  }

  private def newOptional(i: Int): Optional = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Optional(
      Some(true),
      Some(i),
      Some(i),
      Some(BigDecimal(i)),
      Some(s"s$i"),
      Some(ByteString.copyFromUtf8(s"s$i")),
      Some(t.plus(Duration.millis(i))),
      Some(dt.toLocalDate.plusDays(i)),
      Some(dt.toLocalTime.plusMillis(i)),
      Some(dt.toLocalDateTime.plusMillis(i))
    )
  }

  private def newRepeated(i: Int): Repeated = {
    val t = new Instant(0)
    val dt = t.toDateTime(DateTimeZone.UTC)
    Repeated(
      List(true),
      List(i),
      List(i),
      List(BigDecimal(i)),
      List(s"s$i"),
      List(ByteString.copyFromUtf8(s"s$i")),
      List(t.plus(Duration.millis(i))),
      List(dt.toLocalDate.plusDays(i)),
      List(dt.toLocalTime.plusMillis(i)),
      List(dt.toLocalDateTime.plusMillis(i))
    )
  }
}
