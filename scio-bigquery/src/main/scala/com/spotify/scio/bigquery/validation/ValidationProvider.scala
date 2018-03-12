package com.spotify.scio.bigquery.validation

import com.google.api.services.bigquery.model.TableFieldSchema

import scala.reflect.macros.blackbox
import scala.reflect.runtime.universe._


trait ValidationProvider {

  // Returns true if we should override default mapping
  // Uses schema directly from BigQuery loading
  def shouldOverrideType(tfs: TableFieldSchema): Boolean

  // Returns true if we should override default mapping
  // Uses compile time types
  def shouldOverrideType(c: blackbox.Context)(tpe: c.Type): Boolean

  // Returns true if we should override default mapping
  // Uses runtime types
  def shouldOverrideType(tpe: Type): Boolean

  def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree

  def createInstance(c: blackbox.Context)(tpe: c.Type, s: String): c.Tree

  def getBigQueryType(tpe: Type): String
}
