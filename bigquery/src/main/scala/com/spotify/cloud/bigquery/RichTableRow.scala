package com.spotify.cloud.bigquery

class RichTableRow(r: TableRow) {

  def getBoolean(name: AnyRef): Boolean = r.get(name).asInstanceOf[Boolean]

  def getInt(name: AnyRef): Int = this.getString(name).toInt

  def getLong(name: AnyRef): Long = this.getString(name).toLong

  def getFloat(name: AnyRef): Float = this.getString(name).toFloat

  def getDouble(name: AnyRef): Double = this.getString(name).toDouble

  def getString(name: AnyRef): String = r.get(name).toString

}
