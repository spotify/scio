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

sealed trait Source {
  protected type Impl <: Source
  def latest(bq: BigQuery): Impl
  def latest(): Impl = latest(BigQuery.defaultInstance())
}

/** A wrapper type [[Query]] which wraps a SQL String. */
final case class Query(underlying: String) extends Source {
  override protected type Impl = Query

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
  override def latest(bq: BigQuery): Query =
    copy(BigQueryPartitionUtil.latestQuery(bq, underlying))

  override def toString: String = underlying
}

/**
 * Bigquery [[Table]]. Tables can be referenced by a table spec `String` or by a table reference
 * [[GTableReference]]. An additional [[Table.Filter]] can be given to specify selected fields and
 * row restrictions when used with the BQ storage read API.
 *
 * Example: Create a [[Table]] from a [[GTableReference]]:
 * {{{
 *   val tableReference = new TableReference
 *   tableReference.setProjectId("bigquery-public-data")
 *   tableReference.setDatasetId("samples")
 *   tableReference.setTableId("shakespeare")
 *   val table = Table(tableReference)
 * }}}
 * or with a spec string with filtering:
 * {{{
 *   val table = Table(
 *     "bigquery-public-data:samples.shakespeare",
 *     List("word", "word_count"),
 *     "word_count > 10"
 *   )
 * }}}
 *
 * A helper method is provided to replace the "$LATEST" placeholder in the table name to the latest
 * common partition.
 * {{{
 *   val table = Table("some_project:some_data.some_table_$LATEST").latest()
 * }}}
 */
case class Table private (ref: GTableReference, filter: Option[Table.Filter]) extends Source {
  override protected type Impl = Table
  lazy val spec: String = BigQueryHelpers.toTableSpec(ref)
  def latest(bq: BigQuery): Table = {
    val latestSpec = BigQueryPartitionUtil.latestTable(bq, spec)
    val latestRef = BigQueryHelpers.parseTableSpec(latestSpec)
    copy(latestRef)
  }

  override def toString: String = filter match {
    case None => spec
    case Some(Table.Filter(selectedFields, rowRestriction)) =>
      val sb = new StringBuilder("SELECT ")
      selectedFields match {
        case Nil => sb.append("*")
        case _   => sb.append(selectedFields.mkString(","))
      }
      sb.append(" FROM `")
        .append(ref.getProjectId)
        .append(".")
        .append(ref.getDatasetId)
        .append(".")
        .append(ref.getTableId)
        .append("`")
      rowRestriction.foreach(r => sb.append(" WHERE ").append(r))
      sb.result()
  }
}

object Table {

  /**
   * @param selectedFields
   *   names of the fields in the table that should be read. If empty, all fields will be read. If
   *   the specified field is a nested field, all the sub-fields in the field will be selected.
   *   Fields will always appear in the generated class in the same order as they appear in the
   *   table, regardless of the order specified in selectedFields.
   * @param rowRestriction
   *   SQL text filtering statement, similar ti a WHERE clause in a query. Currently, we support
   *   combinations of predicates that are a comparison between a column and a constant value in SQL
   *   statement. Aggregates are not supported. For example:
   *
   * {{{
   *   "a > DATE '2014-09-27' AND (b > 5 AND c LIKE 'date')"
   * }}}
   */
  final case class Filter(
    selectedFields: List[String],
    rowRestriction: Option[String]
  )

  def apply(ref: GTableReference): Table =
    new Table(ref, None)

  def apply(ref: GTableReference, selectedFields: List[String]): Table =
    new Table(ref, Some(Filter(selectedFields, None)))

  def apply(ref: GTableReference, rowRestriction: String): Table =
    new Table(ref, Some(Filter(List.empty, Some(rowRestriction))))

  def apply(ref: GTableReference, selectedFields: List[String], rowRestriction: String): Table =
    new Table(ref, Some(Filter(selectedFields, Some(rowRestriction))))

  def apply(ref: GTableReference, filter: Table.Filter): Table =
    new Table(ref, Some(filter))

  def apply(spec: String): Table =
    new Table(BigQueryHelpers.parseTableSpec(spec), None)

  def apply(spec: String, selectedFields: List[String]): Table =
    new Table(BigQueryHelpers.parseTableSpec(spec), Some(Filter(selectedFields, None)))

  def apply(spec: String, rowRestriction: String): Table =
    new Table(BigQueryHelpers.parseTableSpec(spec), Some(Filter(List.empty, Some(rowRestriction))))

  def apply(spec: String, selectedFields: List[String], rowRestriction: String): Table =
    new Table(
      BigQueryHelpers.parseTableSpec(spec),
      Some(Filter(selectedFields, Some(rowRestriction)))
    )

  def apply(spec: String, filter: Table.Filter): Table =
    new Table(BigQueryHelpers.parseTableSpec(spec), Some(filter))

  def apply(spec: String, filter: Option[Table.Filter]): Table =
    new Table(BigQueryHelpers.parseTableSpec(spec), filter)
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
}

/** Utility for BigQuery `DATE` type. */
object Date {
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
}
