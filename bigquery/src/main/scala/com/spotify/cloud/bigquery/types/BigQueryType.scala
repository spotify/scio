package com.spotify.cloud.bigquery.types

import com.google.api.services.bigquery.model.{TableReference, TableRow, TableSchema}

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.runtime.universe._

/** Macro annotations and converter generators for BigQuery types. */
object BigQueryType {

  /** Trait for companion objects of case classes generated with table. */
  trait HasTable {
    /** Table for case class schema. */
    def table: TableReference
  }

  /** Trait for companion objects of case classes generated with schema. */
  trait HasSchema {
    /** Case class schema. */
    def schema: TableSchema
  }

  /** Trait for companion objects of case classes generated with SELECT query. */
  trait HasQuery {
    /** SELECT query for case class schema. */
    def query: String
  }

  /**
   * Macro annotation for a BigQuery table.
   *
   * Generate case classes for a BigQuery table. Note that `tableSpec` must be a string literal in
   * the form of `project:dataset.table`. For example:
   *
   * {{{
   * @BigQueryType.fromTable("project:dataset.table") class MyRecord
   * }}}
   */
  class fromTable(tableSpec: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.tableImpl
  }

  /**
   * Macro annotation for a BigQuery schema.
   *
   * Generate case classes for a BigQuery schema. Note that `schema` must be a string literal of
   * the JSON schema. For example:
   *
   * {{{
   * @BigQueryType.fromSchema("""
   *   {
   *     "fields": [
   *       {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"},
   *       {"mode": "REQUIRED", "name": "f2", "type": "FLOAT"},
   *       {"mode": "REQUIRED", "name": "f3", "type": "STRING"},
   *       {"mode": "REQUIRED", "name": "f4", "type": "TIMESTAMP"}
   *     ]
   *   }""") class MyRecord
   * }}}
   */
  class fromSchema(schema: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.schemaImpl
  }


  /**
   * Macro annotation for a BigQuery SELECT query.
   *
   * Generate case classes for a BigQuery SELECT query. Note that `schema` must be a string
   * literal of the SELECT query. For example:
   *
   * {{{
   * @BigQueryType.fromQuery("SELECT field1, field2 FROM [project:dataset.table]")
   * }}}
   */
  class fromQuery(query: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.queryImpl
  }

  /**
   * Generate [[com.google.api.services.bigquery.model.TableSchema TableSchema]] for a case class.
   */
  def schemaOf[T: TypeTag]: TableSchema = SchemaProvider.schemaOf[T]

  /** Generate a converter function from [[TableRow]] to the given case class `T`. */
  def fromTableRow[T]: (TableRow => T) = macro ConverterProvider.fromTableRowImpl[T]

  /** Generate a converter function from the given case class `T` to [[TableRow]]. */
  def toTableRow[T]: (T => TableRow) = macro ConverterProvider.toTableRowImpl[T]

}
