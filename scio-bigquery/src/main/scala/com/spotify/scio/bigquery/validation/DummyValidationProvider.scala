package com.spotify.scio.bigquery.validation

import com.google.api.services.bigquery.model.TableFieldSchema

import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe

class DummyValidationProvider extends ValidationProvider {
  override def shouldOverrideType(tfs: TableFieldSchema): Boolean = false

  override def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean = false

  override def shouldOverrideType(tpe: universe.Type): Boolean = false

  override def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree = null

  override def createInstance(c: blackbox.Context)(tpe: c.Type, s: String): c.Tree = null

  override def getBigQueryType(tpe: universe.Type): String = null
}
