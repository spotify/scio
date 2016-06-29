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

import com.google.api.services.bigquery.model.{TableRow => GTableRow}
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.util.{Try, Failure}

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
  // TODO: scala 2.11
  // implicit class RichTableRow(private val r: TableRow) extends AnyVal {
  implicit class RichTableRow(val r: TableRow) {

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

  /** Utility for bigQuery time stamps. */
  object Timestamp {

    private val formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").withZoneUTC()

    /** Convert Instant to BigQuery time stamp string. */
    def apply(instant: Instant): String = formatter.print(instant) + " UTC"

    /** Convert millisecond instant to BigQuery time stamp string. */
    def apply(instant: Long): String = formatter.print(instant) + " UTC"

    private val hasMillisRx = """\.[0-9]+$""".r

    private def tryParse(
      timestamp: String,
      priorException: Option[Throwable] = None
    ): Try[Instant] = {
      // try to parse with DateTime formatter; if failure, check timestamp
      // for missing fractional seconds and potentially try again
      Try(formatter.parseDateTime(timestamp).toInstant).recoverWith {
        case _ if priorException.isDefined =>
          // Pass original exception for the unmodified timestamp, since its error
          // string will reflect the raw timestamp in the user's data
          Failure(priorException.get)

        case ex: IllegalArgumentException if hasMillisRx.findFirstIn(timestamp).isEmpty =>
          // Failure due to missing fractional seconds can be retried,
          // given that we've already checked for earlier failures.  Pass the
          // current exception to be thrown if we fail again, in which case it
          // likely reflects some failure other than missing fractional seconds.
          tryParse(timestamp.stripSuffix(".") + ".000", Some(ex))

        case ex =>
          // Unknown cause, so get out of here
          Failure(ex)
      }
    }

    /** Convert BigQuery time stamp string to Instant. */
    def parse(timestamp: String): Instant = {
      // Erase trailing ' UTC' before parsing
      tryParse(timestamp.stripSuffix(" UTC")).get
    }

  }

}
