package com.spotify.scio.bigquery.types

import scala.collection.mutable.{Map => MMap}

private[types] object NameProvider {
  private val m = MMap.empty[String, Int].withDefaultValue(0)

  /**
   * Generate a unique name for a nested record.
   * This is necessary since we create case classes for nested records and name them with their
   * field names.
   */
  def getUniqueName(name: String): String = m.synchronized {
    val cName = toPascalCase(name) + '$'
    m(cName) += 1
    cName + m(cName)
  }

  private def toPascalCase(s: String): String =
    s.split('_')
      .filter(_.nonEmpty)
      .map(t => s"${t(0).toUpper}${t.drop(1).toLowerCase}")
      .mkString("")
}
