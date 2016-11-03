/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio

import java.time._
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.ChronoField

import com.google.api.services.bigquery.model.{TableRow => GTableRow}

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Main package for BigQuery APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.bigquery._
 * }}}
 */
package object bigquery {

  /**
   * Create a TableRow with Map-like syntax. For example:
   *
   * {{{
   * val r = TableRow("name" -> "Alice", "score" -> 100)
   * }}}
   */
  object TableRow {
    def apply(fields: (String, _)*): TableRow =
      fields.foldLeft(new GTableRow())((r, kv) => r.set(kv._1, kv._2))
  }

  /** Alias for BigQuery TableRow. */
  type TableRow = GTableRow

  /** Enhanced version of TableRow with typed getters. */
  implicit class RichTableRow(val r: TableRow) extends AnyVal {

    def getBoolean(name: AnyRef): Boolean = this.getValue(name, _.toString.toBoolean, false)

    def getLong(name: AnyRef): Long = this.getValue(name, _.toString.toLong, 0L)

    def getDouble(name: AnyRef): Double = this.getValue(name, _.toString.toDouble, 0.0)

    def getString(name: AnyRef): String = this.getValue(name, _.toString, null)

    def getTimestamp(name: AnyRef): Instant =
      this.getValue(name, v => Timestamp.parse(v.toString), null)

    def getRepeated(name: AnyRef): Seq[AnyRef] =
      this.getValue(name, _.asInstanceOf[java.util.List[AnyRef]].asScala, null)

    def getRecord(name: AnyRef): TableRow = r.get(name).asInstanceOf[TableRow]

    private def getValue[T](name: AnyRef, fn: AnyRef => T, default: T): T = {
      val o = r.get(name)
      if (o == null) {
        default
      } else {
        fn(o)
      }
    }

  }

  /** Utility for BigQuery TIMESTAMP type. */
  object Timestamp {

    // FIXME: verify that these match BigQuery specification
    // YYYY-[M]M-[D]D[ [H]H:[M]M:[S]S[.DDDDDD]][time zone]
    // 'T' is required right now for this to work
    // https://github.com/spotify/scio/issues/376
    // https://code.google.com/p/google-cloud-platform/issues/detail?id=185
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS VV")
      .withZone(ZoneOffset.UTC)

    private val parser = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd")
      .appendOptional(new DateTimeFormatterBuilder()
        .appendPattern("[ ]['T']HH:mm:ss")
        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .toFormatter)
      .appendOptional(DateTimeFormatter.ofPattern("[ ][xxx][xx][x][z]"))
      .toFormatter

    /** Convert Instant to BigQuery TIMESTAMP string. */
    def apply(instant: Instant): String = formatter.format(instant)

    /** Convert millisecond instant to BigQuery TIMESTAMP string. */
    def apply(instant: Long): String = apply(Instant.ofEpochMilli(instant))

    /** Convert BigQuery TIMESTAMP string to Instant. */
    def parse(timestamp: String): Instant = {
      val base = parser.parse(timestamp)
      val date = LocalDate.from(base)
      val time = Try(LocalTime.from(base)).getOrElse(LocalTime.MIN)
      val zone = Try(ZoneId.from(base)).getOrElse(ZoneOffset.UTC)
      val zonedDateTime = ZonedDateTime.of(date, time, zone)
      Instant.from(zonedDateTime)
    }

  }

  /** Utility for BigQuery DATE type. */
  object Date {
    // YYYY-[M]M-[D]D
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)

    /** Convert LocalDate to BigQuery DATE string. */
    def apply(date: LocalDate): String = formatter.format(date)

    /** Convert BigQuery DATE string to LocalDate. */
    def parse(date: String): LocalDate = LocalDate.parse(date, formatter)
  }

  /** Utility for BigQuery TIME type. */
  object Time {
    // [H]H:[M]M:[S]S[.DDDDDD]
    private val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS").withZone(ZoneOffset.UTC)
    private val parser = new DateTimeFormatterBuilder()
      .appendPattern("HH:mm:ss")
      .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
      .toFormatter
      .withZone(ZoneOffset.UTC)

    /** Convert LocalTime to BigQuery TIME string. */
    def apply(time: LocalTime): String = formatter.format(time)

    /** Convert BigQuery TIME string to LocalTime. */
    def parse(time: String): LocalTime = LocalTime.parse(time, parser)
  }

  /** Utility for BigQuery DATETIME type. */
  object DateTime {
    // YYYY-[M]M-[D]D[ [H]H:[M]M:[S]S[.DDDDDD]]
    // 'T' is required right now for this to work
    // https://github.com/spotify/scio/issues/376
    // https://code.google.com/p/google-cloud-platform/issues/detail?id=185
    private val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private val parser = new DateTimeFormatterBuilder()
      .appendPattern("yyyy-MM-dd")
      .appendOptional(new DateTimeFormatterBuilder()
        .appendPattern("[ ]['T']HH:mm:ss")
        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .toFormatter)
      .toFormatter

    /** Convert LocalDateTime to BigQuery DATETIME string. */
    def apply(datetime: LocalDateTime): String = formatter.format(datetime)

    /** Convert BigQuery DATETIME string to LocalDateTime. */
    def parse(datetime: String): LocalDateTime = {
      val base = parser.parse(datetime)
      val date = LocalDate.from(base)
      val time = Try(LocalTime.from(base)).getOrElse(LocalTime.MIN)
      LocalDateTime.of(date, time)
    }
  }

}
