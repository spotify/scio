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
import com.spotify.scio.bigquery.syntax.AllSyntax
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}
import org.joda.time.{Instant, LocalDate, LocalDateTime, LocalTime}

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
 * [[com.spotify.scio.bigquery.client.BigQuery.query.rows]],
 * [[com.spotify.scio.bigquery.client.BigQuery.query.schema]],
 * [[com.spotify.scio.bigquery.client.BigQuery.getTypedRows]] and
 * [[com.spotify.scio.bigquery.BigQueryType.fromQuery BigQueryType.fromQuery]], automatically
 * detects the query's dialect. To override this, start the query with either `#legacysql` or
 * `#standardsql` comment line.
 */
package object bigquery extends AllSyntax {

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

    def getBoolean(name: AnyRef): Boolean =
      this.getValue(name, _.toString.toBoolean, false)

    def getBooleanOpt(name: AnyRef): Option[Boolean] =
      this.getValueOpt(name, _.toString.toBoolean)

    def getLong(name: AnyRef): Long = this.getValue(name, _.toString.toLong, 0L)

    def getLongOpt(name: AnyRef): Option[Long] =
      this.getValueOpt(name, _.toString.toLong)

    def getDouble(name: AnyRef): Double =
      this.getValue(name, _.toString.toDouble, 0.0)

    def getDoubleOpt(name: AnyRef): Option[Double] =
      this.getValueOpt(name, _.toString.toDouble)

    def getString(name: AnyRef): String = this.getValue(name, _.toString, null)

    def getStringOpt(name: AnyRef): Option[String] =
      this.getValueOpt(name, _.toString)

    def getTimestamp(name: AnyRef): Instant =
      this.getValue(name, v => Timestamp.parse(v.toString), null)

    def getTimestampOpt(name: AnyRef): Option[Instant] =
      this.getValueOpt(name, v => Timestamp.parse(v.toString))

    def getDate(name: AnyRef): LocalDate =
      this.getValue(name, v => Date.parse(v.toString), null)

    def getDateOpt(name: AnyRef): Option[LocalDate] =
      this.getValueOpt(name, v => Date.parse(v.toString))

    def getTime(name: AnyRef): LocalTime =
      this.getValue(name, v => Time.parse(v.toString), null)

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

}
