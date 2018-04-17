/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.scio.bigquery.types

import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import org.apache.avro.generic.GenericRecord

import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.runtime.universe._
import scala.util.Try

/**
 * Macro annotations and converter generators for BigQuery types.
 *
 * The following table lists each legacy SQL data type, its standard SQL equivalent and Scala type.
 * {{{
 * Legacy SQL      Standard SQL    Scala type
 * BOOLEAN         BOOL            Boolean
 * INTEGER         INT64           Long, Int
 * FLOAT           FLOAT64         Double, Float
 * STRING          STRING          String
 * BYTES           BYTES           com.google.protobuf.ByteString, Array[Byte]
 * RECORD          STRUCT          Nested case class
 * REPEATED        ARRAY           List[T]
 * TIMESTAMP       TIMESTAMP       org.joda.time.Instant
 * DATE            DATE            org.joda.time.LocalDate
 * TIME            TIME            org.joda.time.LocalTime
 * DATETIME        DATETIME        org.joda.time.LocalDateTime
 * }}}
 *
 * @groupname trait Traits for annotated types
 * @groupname annotation Type annotations
 * @groupname converters Converters
 * @groupname Ungrouped Other Members
 */
object BigQueryType {

  /**
   * Trait for companion objects of case classes generated with table.
   * @group trait
   */
  trait HasTable {
    /** Table for case class schema. */
    def table: String
  }

  /**
   * Trait for companion objects of case classes generated with table.
   * Instance of this trait are provided as implicits alowing static discovery.
   * That trait provide evidence that a BQ table is statically known for a given type T.
   * @group trait
   */
  trait Table[T] extends HasTable

  /**
   * Trait for companion objects of case classes generated with schema.
   * @group trait
   */
  trait HasSchema[T] {
    /** Case class schema. */
    def schema: TableSchema

    /** Avro [[GenericRecord]] to `T` converter. */
    def fromAvro: (GenericRecord => T)

    /** TableRow to `T` converter. */
    def fromTableRow: (TableRow => T)

    /** `T` to TableRow converter. */
    def toTableRow: (T => TableRow)

    /** Get a pretty string representation of the schema. */
    def toPrettyString(indent: Int = 0): String
  }

  /**
   * Trait for companion objects of case classes generated with SELECT query.
   * @group trait
   */
  trait HasQuery {
    /** SELECT query for case class schema. */
    def query: String
  }

  /**
   * Trait for companion objects of case classes generated with query.
   * Instance of this trait are provided as implicits alowing static discovery.
   * That trait provide evidence that a BQ query is statically known for a given type T.
   * @group trait
   */
  trait Query[T] extends HasQuery

  /**
   * Trait for companion objects of case classes generated with table description.
   * @group trait
   */
  trait HasTableDescription {
    /** Case class table description. */
    def tableDescription: String
  }

  /**
   * Trait for case classes with generated companion objects.
   * @group trait
   */
  trait HasAnnotation

  /**
   * Macro annotation for a BigQuery table.
   *
   * Generate case classes for a BigQuery table. Note that `tableSpec` must be a string literal in
   * the form of `project:dataset.table` with optional `.stripMargin` at the end. For example:
   *
   * {{{
   * @BigQueryType.fromTable("project:dataset.table") class MyRecord
   * }}}
   *
   * String formatting syntax can be used in `tableSpec` when additional `args` are supplied. For
   * example:
   *
   * {{{
   * @BigQueryType.fromTable("project:dataset.%s", "table")
   * }}}
   *
   * "\$LATEST" can be used as a placeholder for table partitions. The latest partition available
   * will be used. For example:
   *
   * {{{
   * @BigQueryType.fromTable("project:dataset.table_%s", "\$LATEST")
   * }}}
   *
   * Also generate a companion object with convenience methods.
   * @group annotation
   */
  class fromTable(tableSpec: String, args: String*) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.tableImpl
  }

  /**
   * Macro annotation for a BigQuery schema.
   *
   * Generate case classes for a BigQuery schema. Note that `schema` must be a string literal of
   * the JSON schema with optional `.stripMargin` at the end. For example:
   *
   * {{{
   * @BigQueryType.fromSchema(
   *   """
   *     |{
   *     |  "fields": [
   *     |    {"mode": "REQUIRED", "name": "f1", "type": "INTEGER"},
   *     |    {"mode": "REQUIRED", "name": "f2", "type": "FLOAT"},
   *     |    {"mode": "REQUIRED", "name": "f3", "type": "STRING"},
   *     |    {"mode": "REQUIRED", "name": "f4", "type": "TIMESTAMP"}
   *     |  ]
   *     |}
   *   """.stripMargin) class MyRecord
   * }}}
   *
   * Also generate a companion object with convenience methods.
   * @group annotation
   */
  class fromSchema(schema: String) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.schemaImpl
  }

  /**
   * Macro annotation for a BigQuery SELECT query.
   *
   * Generate case classes for a BigQuery SELECT query. Note that `query` must be a string
   * literal of the SELECT query with optional `.stripMargin` at the end. For example:
   *
   * {{{
   * @BigQueryType.fromQuery("SELECT field1, field2 FROM [project:dataset.table]")
   * }}}
   *
   * String formatting syntax can be used in `query` when additional `args` are supplied. For
   * example:
   *
   * {{{
   * @BigQueryType.fromQuery("SELECT field1, field2 FROM [%s]", "table")
   * }}}
   *
   * "\$LATEST" can be used as a placeholder for table partitions. The latest common partition
   * available for all tables with the placeholder will be used. For example:
   *
   * {{{
   * @BigQueryType.fromQuery(
   *   "SELECT field1, field2 FROM [project:d1.t1_%s] JOIN [project:d2.t2_%s] USING field3",
   *   "\$LATEST", "\$LATEST")
   * }}}
   *
   * Also generate a companion object with convenience methods.
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   * @group annotation
   */
  class fromQuery(query: String, args: String*) extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.queryImpl
  }

  /**
   * Macro annotation for case classes to be saved to a BigQuery table.
   *
   * Note that this annotation does not generate case classes, only a companion object with
   * convenience methods. You need to define a complete case class for as output record. For
   * example:
   *
   * {{{
   * @BigQueryType.toTable
   * case class Result(name: String, score: Double)
   * }}}
   * @group annotation
   */
  class toTable extends StaticAnnotation {
    def macroTransform(annottees: Any*): Any = macro TypeProvider.toTableImpl
  }

  /**
   * Generate [[com.google.api.services.bigquery.model.TableSchema TableSchema]] for a case class.
   */
  def schemaOf[T: TypeTag]: TableSchema = SchemaProvider.schemaOf[T]

  /**
    * Generate a converter function from Avro [[GenericRecord]] to the given case class `T`.
    * @group converters
    */
  def fromAvro[T]: (GenericRecord => T) = macro ConverterProvider.fromAvroImpl[T]

  /**
   * Generate a converter function from [[TableRow]] to the given case class `T`.
   * @group converters
   */
  def fromTableRow[T]: (TableRow => T) = macro ConverterProvider.fromTableRowImpl[T]

  /**
   * Generate a converter function from the given case class `T` to [[TableRow]].
   * @group converters
   */
  def toTableRow[T]: (T => TableRow) = macro ConverterProvider.toTableRowImpl[T]

  /** Create a new BigQueryType instance. */
  def apply[T: TypeTag]: BigQueryType[T] = new BigQueryType[T]

}

/**
 * Type class for case class `T` annotated for BigQuery IO.
 *
 * This decouples generated fields and methods from macro expansion to keep core macro free.
 */
class BigQueryType[T: TypeTag] {

  private val bases = typeOf[T].companion.baseClasses

  private val instance = runtimeMirror(getClass.getClassLoader)
    .reflectModule(typeOf[T].typeSymbol.companion.asModule)
    .instance

  private def getField(key: String) = instance.getClass.getMethod(key).invoke(instance)

  /** Whether the case class is annotated for a table. */
  def isTable: Boolean = bases.contains(typeOf[BigQueryType.HasTable].typeSymbol)

  /** Whether the case class is annotated for a query. */
  def isQuery: Boolean = bases.contains(typeOf[BigQueryType.HasQuery].typeSymbol)

  /** Table reference from the annotation. */
  def table: Option[String] = Try(getField("table").asInstanceOf[String]).toOption

  /** Query from the annotation. */
  def query: Option[String] = Try(getField("query").asInstanceOf[String]).toOption

  /** Table description from the annotation. */
  def tableDescription: Option[String] =
    Try(getField("tableDescription").asInstanceOf[String]).toOption

  /** Avro [[GenericRecord]] to `T` converter. */
  def fromAvro: (GenericRecord => T) = getField("fromAvro").asInstanceOf[(GenericRecord => T)]

  /** TableRow to `T` converter. */
  def fromTableRow: (TableRow => T) = getField("fromTableRow").asInstanceOf[(TableRow => T)]

  /** `T` to TableRow converter. */
  def toTableRow: (T => TableRow) = getField("toTableRow").asInstanceOf[(T => TableRow)]

  /** TableSchema of `T`. */
  def schema: TableSchema = BigQueryType.schemaOf[T]

}
