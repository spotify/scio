package com.spotify.cloud

import com.google.api.services.bigquery.model.{TableRow => GTableRow}

package object bigquery {

  object TableRow {
    def apply(fields: (String, _)*): TableRow = fields.foldLeft(new GTableRow())((r, kv) => r.set(kv._1, kv._2))
  }

  type TableRow = GTableRow

  implicit class RichTableRow(val r: TableRow) extends AnyVal {

    def getBoolean(name: AnyRef): Boolean = r.get(name).asInstanceOf[Boolean]

    def getInt(name: AnyRef): Int = this.getString(name).toInt

    def getLong(name: AnyRef): Long = this.getString(name).toLong

    def getFloat(name: AnyRef): Float = this.getString(name).toFloat

    def getDouble(name: AnyRef): Double = this.getString(name).toDouble

    def getString(name: AnyRef): String = r.get(name).toString

  }

}
