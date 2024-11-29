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

import com.google.api.services.bigquery.model.{
  Clustering => GClustering,
  TableReference => GTableReference,
  TableRow => GTableRow,
  TimePartitioning => GTimePartitioning
}
import com.spotify.scio.bigquery.client.BigQuery
import org.apache.avro.Conversions.DecimalConversion
import org.apache.avro.LogicalTypes
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, DateTimeFormatterBuilder}

import java.math.MathContext
import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._

sealed trait Source

/** A wrapper type [[Query]] which wraps a SQL String. */
final case class Query(underlying: String) extends Source {

  /**
   * A helper method to replace the "$LATEST" placeholder in query to the latest common partition.
   * For example:
   * {{{
   *   @BigQueryType.fromQuery("SELECT ... FROM `project.data.foo_%s`", "$LATEST")
   *   class Foo
   *
   *   val bq: BigQuery = BigQuery.defaultInstance()
   *   scioContext.bigQuerySelect(Foo.queryAsSource("$LATEST").latest(bq))
   * }}}
   *
   * Or, if your query string is a dynamic value which uses a "$LATEST" placeholder,
   * {{{
   *   val dynamicSQLStr = "SELECT ... FROM some_table_$LATEST"
   *   scioContext.bigQuerySelect(Query(dynamicSQLStr).latest(bq))
   * }}}
   *
   * @param bq
   *   [[BigQuery]] client
   * @return
   *   [[Query]] with "$LATEST" replaced
   */
  def latest(bq: BigQuery): Query =
    Query(BigQueryPartitionUtil.latestQuery(bq, underlying))

  def latest(): Query = latest(BigQuery.defaultInstance())
}

/**
 * [[Table]] abstracts the multiple ways of referencing Bigquery tables. Tables can be referenced by
 * a table spec `String` or by a table reference [[GTableReference]].
 *
 * Example:
 * {{{
 *   val table = Table.Spec("bigquery-public-data:samples.shakespeare")
 *   sc.bigQueryTable(table)
 *     .filter(r => "hamlet".equals(r.getString("corpus")) && "Polonius".equals(r.getString("word")))
 *     .saveAsTextFile("./output.txt")
 *   sc.run()
 * }}}
 *
 * Or create a [[Table]] from a [[GTableReference]]:
 * {{{
 *   val tableReference = new TableReference
 *   tableReference.setProjectId("bigquery-public-data")
 *   tableReference.setDatasetId("samples")
 *   tableReference.setTableId("shakespeare")
 *   val table = Table.Ref(tableReference)
 * }}}
 *
 * A helper method is provided to replace the "$LATEST" placeholder in the table name to the latest
 * common partition.
 * {{{
 *   val table = Table.Spec("some_project:some_data.some_table_$LATEST").latest()
 * }}}
 */
sealed trait Table extends Source {
  def spec: String

  def ref: GTableReference

  def latest(bg: BigQuery): Table

  def latest(): Table
}

object Table {
  final case class Ref(ref: GTableReference) extends Table {
    override lazy val spec: String = BigQueryHelpers.toTableSpec(ref)
    def latest(bq: BigQuery): Ref =
      Ref(Spec(spec).latest(bq).ref)
    def latest(): Ref = latest(BigQuery.defaultInstance())

  }
  final case class Spec(spec: String) extends Table {
    override val ref: GTableReference = BigQueryHelpers.parseTableSpec(spec)
    def latest(bq: BigQuery): Spec =
      Spec(BigQueryPartitionUtil.latestTable(bq, spec))
    def latest(): Spec = latest(BigQuery.defaultInstance())
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

/** Utility for BigQuery `TIMESTAMP` type. */
object Timestamp {
  // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]][time zone]
  private[this] val Formatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS ZZZ")

  private[this] val Parser = new DateTimeFormatterBuilder()
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
  def apply(instant: Instant): String = Formatter.print(instant)

  /** Convert millisecond instant to BigQuery `TIMESTAMP` string. */
  def apply(instant: Long): String = Formatter.print(instant)

  /** Convert BigQuery `TIMESTAMP` string to `Instant`. */
  def parse(timestamp: String): Instant =
    Parser.parseDateTime(timestamp).toInstant

  // For BigQueryType macros only, do not use directly
  def parse(timestamp: Any): Instant = timestamp match {
    case t: Long => new Instant(t / 1000)
    case _       => parse(timestamp.toString)
  }

  def micros(timestamp: Instant): Long = timestamp.getMillis * 1000
}

/** Utility for BigQuery `DATE` type. */
object Date {
  private val EpochDate = new LocalDate(1970, 1, 1)
  // YYYY-[M]M-[D]D
  private[this] val Formatter =
    DateTimeFormat.forPattern("yyyy-MM-dd").withZoneUTC()

  /** Convert `LocalDate` to BigQuery `DATE` string. */
  def apply(date: LocalDate): String = Formatter.print(date)

  /** Convert BigQuery `DATE` string to `LocalDate`. */
  def parse(date: String): LocalDate = LocalDate.parse(date, Formatter)

  // For BigQueryType macros only, do not use directly
  def parse(date: Any): LocalDate = date match {
    case d: Int => new LocalDate(0, DateTimeZone.UTC).plusDays(d)
    case _      => parse(date.toString)
  }

  def days(date: LocalDate): Int = Days.daysBetween(EpochDate, date).getDays
}

/** Utility for BigQuery `TIME` type. */
object Time {
  // [H]H:[M]M:[S]S[.DDDDDD]
  private[this] val Formatter =
    DateTimeFormat.forPattern("HH:mm:ss.SSSSSS").withZoneUTC()
  private[this] val Parser = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("HH:mm:ss").getParser)
    .appendOptional(DateTimeFormat.forPattern(".SSSSSS").getParser)
    .toFormatter
    .withZoneUTC()

  /** Convert `LocalTime` to BigQuery `TIME` string. */
  def apply(time: LocalTime): String = Formatter.print(time)

  /** Convert BigQuery `TIME` string to `LocalTime`. */
  def parse(time: String): LocalTime = Parser.parseLocalTime(time)

  // For BigQueryType macros only, do not use directly
  def parse(time: Any): LocalTime = time match {
    case t: Long => new LocalTime(t / 1000, DateTimeZone.UTC)
    case _       => parse(time.toString)
  }

  def micros(time: LocalTime): Long = time.millisOfDay().get().toLong * 1000
}

/** Utility for BigQuery `DATETIME` type. */
object DateTime {
  // YYYY-[M]M-[D]D[( |T)[H]H:[M]M:[S]S[.DDDDDD]]
  private[this] val Formatter =
    DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")

  private[this] val Parser = new DateTimeFormatterBuilder()
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
  def apply(datetime: LocalDateTime): String = Formatter.print(datetime)

  /** Convert BigQuery `DATETIME` string to `LocalDateTime`. */
  def parse(datetime: String): LocalDateTime =
    Parser.parseLocalDateTime(datetime)

  //  For BigQueryType macros only, do not use directly
  def format(datetime: LocalDateTime): String = apply(datetime)
}

/** Scala wrapper for [[com.google.api.services.bigquery.model.TimePartitioning]]. */
final case class TimePartitioning(
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

/** Scala wrapper for [[com.google.api.services.bigquery.model.Clustering]]. */
final case class Clustering(
  fields: Seq[String] = Seq()
) {
  def asJava: GClustering =
    new GClustering()
      .setFields(fields.asJava)
}

/** Scala representation for BQ write sharding. */
sealed trait Sharding
object Sharding {

  /**
   * enables using a dynamically determined number of shards to write to BigQuery. This can be used
   * for both BigQueryIO.Write.Method.FILE_LOADS and BigQueryIO.Write.Method.STREAMING_INSERTS. Only
   * applicable to unbounded data.
   */
  case object Auto extends Sharding

  /**
   * Control how many file shards are written when using BigQuery load jobs, or how many parallel
   * streams are used when using Storage API writes.
   */
  final case class Manual(numShards: Int) extends Sharding
}

object Numeric {
  val MaxNumericPrecision = 38
  val MaxNumericScale = 9

  private val DecimalConverter = new DecimalConversion
  private val DecimalLogicalType = LogicalTypes.decimal(MaxNumericPrecision, MaxNumericScale)

  def apply(value: String): BigDecimal = apply(BigDecimal(value))

  def apply(value: BigDecimal): BigDecimal = {
    val scaled = if (value.scale > MaxNumericScale) {
      value.setScale(MaxNumericScale, scala.math.BigDecimal.RoundingMode.HALF_UP)
    } else {
      value
    }
    require(
      scaled.precision <= MaxNumericPrecision,
      s"max allowed precision is $MaxNumericPrecision"
    )

    scaled.round(new MathContext(MaxNumericPrecision))
  }

  // For BigQueryType macros only, do not use directly
  def parse(value: Any): BigDecimal = value match {
    case b: ByteBuffer => DecimalConverter.fromBytes(b, null, DecimalLogicalType)
    case _             => apply(value.toString)
  }

  def bytes(value: BigDecimal): ByteBuffer =
    DecimalConverter.toBytes(value.bigDecimal, null, DecimalLogicalType)
}
