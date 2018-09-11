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

import com.spotify.scio.values.SCollection
import com.google.api.services.bigquery.model.{TableReference, TableRow => GTableRow}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

import scala.language.implicitConversions
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Main package for BigQuery APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.bigquery._
 * }}}
 *
 * There are two BigQuery dialects,
 * [[https://cloud.google.com/bigquery/docs/reference/legacy-sql legacy]] and
 * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ standard]].
 * APIs that take a BigQuery query string as argument, e.g.
 * [[com.spotify.scio.bigquery.BigQueryClient.getQueryRows BigQueryClient.getQueryRows]],
 * [[com.spotify.scio.bigquery.BigQueryClient.getQuerySchema BigQueryClient.getQuerySchema]],
 * [[com.spotify.scio.bigquery.BigQueryClient.getTypedRows BigQueryClient.getTypedRows]] and
 * [[com.spotify.scio.bigquery.BigQueryType.fromQuery BigQueryType.fromQuery]], automatically
 * detects the query's dialect. To override this, start the query with either `#legacysql` or
 * `#standardsql` comment line.
 */
package object bigquery {

  /** Alias for BigQuery `CreateDisposition`. */
  val CREATE_IF_NEEDED = Write.CreateDisposition.CREATE_IF_NEEDED

  /** Alias for BigQuery `CreateDisposition`. */
  val CREATE_NEVER = Write.CreateDisposition.CREATE_NEVER

  /** Alias for BigQuery `WriteDisposition`. */
  val WRITE_APPEND = Write.WriteDisposition.WRITE_APPEND

  /** Alias for BigQuery `WriteDisposition`. */
  val WRITE_EMPTY = Write.WriteDisposition.WRITE_EMPTY

  /** Alias for BigQuery `WriteDisposition`. */
  val WRITE_TRUNCATE = Write.WriteDisposition.WRITE_TRUNCATE

  /** Typed BigQuery annotations and converters. */
  val BigQueryType = com.spotify.scio.bigquery.types.BigQueryType

  /** BigQuery tag for macro generated classes/fields. */
  type BigQueryTag = com.spotify.scio.bigquery.types.BigQueryTag

  /**
   * Annotation for BigQuery field [[com.spotify.scio.bigquery.types.description description]].
   */
  type description = com.spotify.scio.bigquery.types.description

  implicit def toBigQueryScioContext(c: ScioContext): BigQueryScioContext =
    new BigQueryScioContext(c)
  implicit def toBigQuerySCollection[T](c: SCollection[T]): BigQuerySCollection[T] =
    new BigQuerySCollection[T](c)

  /**
   * Enhanced version of [[com.google.api.services.bigquery.model.TableReference TableReference]].
   */
  implicit class RichTableReference(private val r: TableReference) extends AnyVal {

    /**
     * Return table specification in the form of "[project_id]:[dataset_id].[table_id]" or
     * "[dataset_id].[table_id]".
     */
    def asTableSpec: String = {
      require(r.getDatasetId != null, "Dataset can't be null")
      require(r.getTableId != null, "Table can't be null")

      if (r.getProjectId != null) {
        s"${r.getProjectId}:${r.getDatasetId}.${r.getTableId}"
      } else {
        s"${r.getDatasetId}.${r.getTableId}"
      }
    }
  }

  /**
   * Create a [[TableRow]] with `Map`-like syntax. For example:
   *
   * {{{
   * val r = TableRow("name" -> "Alice", "score" -> 100)
   * }}}
   */
  object TableRow {
    @inline def apply(fields: (String, _)*): TableRow =
      fields.foldLeft(new GTableRow())((r, kv) => r.set(kv._1, kv._2))
  }

  /** Alias for BigQuery `TableRow`. */
  type TableRow = GTableRow

  /** Enhanced version of [[TableRow]] with typed getters. */
  implicit class RichTableRow(private val r: TableRow) extends AnyVal {

    def getBoolean(name: AnyRef): Boolean = this.getValue(name, _.toString.toBoolean, false)

    def getBooleanOpt(name: AnyRef): Option[Boolean] =
      this.getValueOpt(name, _.toString.toBoolean)

    def getLong(name: AnyRef): Long = this.getValue(name, _.toString.toLong, 0L)

    def getLongOpt(name: AnyRef): Option[Long] = this.getValueOpt(name, _.toString.toLong)

    def getDouble(name: AnyRef): Double = this.getValue(name, _.toString.toDouble, 0.0)

    def getDoubleOpt(name: AnyRef): Option[Double] = this.getValueOpt(name, _.toString.toDouble)

    def getString(name: AnyRef): String = this.getValue(name, _.toString, null)

    def getStringOpt(name: AnyRef): Option[String] = this.getValueOpt(name, _.toString)

    def getTimestamp(name: AnyRef): Instant =
      this.getValue(name, v => Timestamp.parse(v.toString), null)

    def getTimestampOpt(name: AnyRef): Option[Instant] =
      this.getValueOpt(name, v => Timestamp.parse(v.toString))

    def getDate(name: AnyRef): LocalDate = this.getValue(name, v => Date.parse(v.toString), null)

    def getDateOpt(name: AnyRef): Option[LocalDate] =
      this.getValueOpt(name, v => Date.parse(v.toString))

    def getTime(name: AnyRef): LocalTime = this.getValue(name, v => Time.parse(v.toString), null)

    def getTimeOpt(name: AnyRef): Option[LocalTime] =
      this.getValueOpt(name, v => Time.parse(v.toString))

    def getDateTime(name: AnyRef): LocalDateTime =
      this.getValue(name, v => DateTime.parse(v.toString), null)

    def getDateTimeOpt(name: AnyRef): Option[LocalDateTime] =
      this.getValueOpt(name, v => DateTime.parse(v.toString))

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

    private def getValueOpt[T](name: AnyRef, fn: AnyRef => T): Option[T] = {
      val o = r.get(name)
      if (o == null) {
        None
      } else {
        Try(fn(o)).toOption
      }
    }

  }

  /** Utility for BigQuery `TIMESTAMP` type. */
  object Timestamp {
    // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
    private[this] val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS ZZZ")

    private[this] val parser = new DateTimeFormatterBuilder()
      .append(DateTimeFormat.forPattern("yyyy-MM-dd"))
      .appendOptional(new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern(" HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser)
      .appendOptional(new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern("'T'HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser)
      .appendOptional(new DateTimeFormatterBuilder()
        .append(null, Array(" ZZZ", "ZZ").map(p => DateTimeFormat.forPattern(p).getParser))
        .toParser)
      .toFormatter
      .withZoneUTC()

    /** Convert `Instant` to BigQuery `TIMESTAMP` string. */
    def apply(instant: Instant): String = formatter.print(instant)

    /** Convert millisecond instant to BigQuery `TIMESTAMP` string. */
    def apply(instant: Long): String = formatter.print(instant)

    /** Convert BigQuery `TIMESTAMP` string to `Instant`. */
    def parse(timestamp: String): Instant = parser.parseDateTime(timestamp).toInstant

  }

  /** Utility for BigQuery `DATE` type. */
  object Date {
    // YYYY-[M]M-[D]D
    private[this] val formatter = DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()

    /** Convert `LocalDate` to BigQuery `DATE` string. */
    def apply(date: LocalDate): String = formatter.print(date)

    /** Convert BigQuery `DATE` string to `LocalDate`. */
    def parse(date: String): LocalDate = LocalDate.parse(date, formatter)
  }

  /** Utility for BigQuery `TIME` type. */
  object Time {
    // [H]H:[M]M:[S]S[.DDDDDD]
    private[this] val formatter = DateTimeFormat.forPattern("HH:mm:ss.SSSSSS").withZoneUTC()
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
    private[this] val formatter = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

    private[this] val parser = new DateTimeFormatterBuilder()
      .append(DateTimeFormat.forPattern("yyyy-MM-dd"))
      .appendOptional(new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern(" HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser)
      .appendOptional(new DateTimeFormatterBuilder()
        .append(DateTimeFormat.forPattern("'T'HH:mm:ss").getParser)
        .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
        .toParser)
      .toFormatter
      .withZoneUTC()

    /** Convert `LocalDateTime` to BigQuery `DATETIME` string. */
    def apply(datetime: LocalDateTime): String = formatter.print(datetime)

    /** Convert BigQuery `DATETIME` string to `LocalDateTime`. */
    def parse(datetime: String): LocalDateTime = parser.parseLocalDateTime(datetime)
  }

}
