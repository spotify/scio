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

import java.math.MathContext

import com.google.api.services.bigquery.model.{TimePartitioning => GTimePartitioning}
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

/** Utility for BigQuery `TIMESTAMP` type. */
object Timestamp {
  // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
  private[this] val formatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS ZZZ")

  private[this] val parser = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("yyyy-MM-dd"))
    .appendOptional(
      new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern(" HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser
    )
    .appendOptional(
      new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern("'T'HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser
    )
    .appendOptional(
      new DateTimeFormatterBuilder()
        .append(null, Array(" ZZZ", "ZZ").map(p => DateTimeFormat.forPattern(p).getParser))
        .toParser
    )
    .toFormatter
    .withZoneUTC()

  /** Convert `Instant` to BigQuery `TIMESTAMP` string. */
  def apply(instant: Instant): String = formatter.print(instant)

  /** Convert millisecond instant to BigQuery `TIMESTAMP` string. */
  def apply(instant: Long): String = formatter.print(instant)

  /** Convert BigQuery `TIMESTAMP` string to `Instant`. */
  def parse(timestamp: String): Instant =
    parser.parseDateTime(timestamp).toInstant

}

/** Utility for BigQuery `DATE` type. */
object Date {
  // YYYY-[M]M-[D]D
  private[this] val formatter =
    DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()

  /** Convert `LocalDate` to BigQuery `DATE` string. */
  def apply(date: LocalDate): String = formatter.print(date)

  /** Convert BigQuery `DATE` string to `LocalDate`. */
  def parse(date: String): LocalDate = LocalDate.parse(date, formatter)
}

/** Utility for BigQuery `TIME` type. */
object Time {
  // [H]H:[M]M:[S]S[.DDDDDD]
  private[this] val formatter =
    DateTimeFormat.forPattern("HH:mm:ss.SSSSSS").withZoneUTC()
  private[this] val parser = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("HH:mm:ss").getParser)
    .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
    .toFormatter
    .withZoneUTC()

  /** Convert `LocalTime` to BigQuery `TIME` string. */
  def apply(time: LocalTime): String = formatter.print(time)

  /** Convert BigQuery `TIME` string to `LocalTime`. */
  def parse(time: String): LocalTime = parser.parseLocalTime(time)
}

/** Utility for BigQuery `DATETIME` type. */
object DateTime {
  // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
  private[this] val formatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

  private[this] val parser = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("yyyy-MM-dd"))
    .appendOptional(
      new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern(" HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser
    )
    .appendOptional(
      new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern("'T'HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser
    )
    .toFormatter
    .withZoneUTC()

  /** Convert `LocalDateTime` to BigQuery `DATETIME` string. */
  def apply(datetime: LocalDateTime): String = formatter.print(datetime)

  /** Convert BigQuery `DATETIME` string to `LocalDateTime`. */
  def parse(datetime: String): LocalDateTime =
    parser.parseLocalDateTime(datetime)
}

/**
 * Scala wrapper for [[com.google.api.services.bigquery.model.TimePartitioning]].
 */
case class TimePartitioning(
  `type`: String,
  field: String = null,
  expirationMs: Long = 0,
  requirePartitionFilter: Boolean = false
) {
  def asJava: GTimePartitioning = {
    var p = new GTimePartitioning()
      .setType(`type`)
      .setRequirePartitionFilter(requirePartitionFilter)
    if (field != null) p = p.setField(field)
    if (expirationMs > 0) p = p.setExpirationMs(expirationMs)
    p
  }
}

object Numeric {
  val MaxNumericPrecision = 38
  val MaxNumericScale = 9

  def apply(value: String): BigDecimal = apply(BigDecimal(value))

  def apply(value: BigDecimal): BigDecimal = {
    // NUMERIC's max scale is 9, precision is 38
    val scaled = if (value.scale > MaxNumericScale) {
      value.setScale(MaxNumericScale, scala.math.BigDecimal.RoundingMode.HALF_UP)
    } else {
      value
    }
    require(
      scaled.precision <= MaxNumericPrecision,
      s"max allowed precision is $MaxNumericPrecision"
    )

    BigDecimal(scaled.toString, new MathContext(MaxNumericPrecision))
  }
}
