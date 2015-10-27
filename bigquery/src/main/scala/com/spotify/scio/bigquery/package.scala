package com.spotify.scio

import com.google.api.services.bigquery.model.{TableRow => GTableRow}

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
  // implicit class RichTableRow(val r: TableRow) extends AnyVal {
  implicit class RichTableRow(val r: TableRow) {

    def getBoolean(name: AnyRef): Boolean = r.get(name).asInstanceOf[Boolean]

    def getInt(name: AnyRef): Int = this.getString(name).toInt

    def getLong(name: AnyRef): Long = this.getString(name).toLong

    def getFloat(name: AnyRef): Float = this.getString(name).toFloat

    def getDouble(name: AnyRef): Double = this.getString(name).toDouble

    def getString(name: AnyRef): String = r.get(name).toString

  }

}
