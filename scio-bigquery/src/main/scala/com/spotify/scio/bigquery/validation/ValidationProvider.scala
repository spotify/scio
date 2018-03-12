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

  // Returns a context.Tree representing the Scala type
  // This is called at macro compile time
  def getScalaType(c: blackbox.Context)(tfs: TableFieldSchema): c.Tree

  // Returns a context.Tree representing a new instance of whatever type is appropriate
  // This is called at macro compile time
  def createInstance(c: blackbox.Context)(tpe: c.Type, s: String): c.Tree

  // Returns the String representation of the BigQuery column type
  // This is called at runtime
  def getBigQueryType(tpe: Type): String
}
