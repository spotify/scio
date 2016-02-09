package com.spotify.scio

import com.google.api.services.bigquery.model.{TableRow => GTableRow}
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._

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
    def apply(fields: (String, _)*): TableRow = fields.foldLeft(new GTableRow())((r, kv) => r.set(kv._1, kv._2))
  }

  /** Alias for BigQuery TableRow. */
  type TableRow = GTableRow

  /** Enhanced version of TableRow with typed getters. */
  // TODO: scala 2.11
  // implicit class RichTableRow(private val r: TableRow) extends AnyVal {
  implicit class RichTableRow(val r: TableRow) {

    def getBoolean(name: AnyRef): Boolean = this.getValue(name, _.toString.toBoolean, false)

    def getInt(name: AnyRef): Int = this.getValue(name, _.toString.toInt, 0)

    def getLong(name: AnyRef): Long = this.getValue(name, _.toString.toLong, 0L)

    def getFloat(name: AnyRef): Float = this.getValue(name, _.toString.toFloat, 0.0f)

    def getDouble(name: AnyRef): Double = this.getValue(name, _.toString.toDouble, 0.0)

    def getString(name: AnyRef): String = this.getValue(name, _.toString, null)

    def getTimestamp(name: AnyRef): Instant = this.getValue(name, v => Timestamp.parse(v.toString), null)

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

    /** Convert BigQuery time stamp string to Instant. */
    def parse(timestamp: String): Instant = formatter.parseDateTime(timestamp.replaceAll(" UTC$", "")).toInstant

  }

}
