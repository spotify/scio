package com.spotify.scio.bigquery.validation

import scala.collection.mutable
import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe._

object Index {
  def getIndexCompileTimeTypes(c: blackbox.Context): mutable.Map[c.Type, Class[_]] = {
    import c.universe._
    mutable.Map[Type, Class[_]](typeOf[Country] -> classOf[Country])
  }

  def getIndexClass: mutable.Map[String, Class[_]] = {
    mutable.Map[String, Class[_]](Country.semanticType -> classOf[Country])
  }

  def getIndexRuntimeTypes: mutable.Map[Type, Class[_]] = {
    mutable.Map[Type, Class[_]](typeOf[Country] -> classOf[Country])
  }

}
