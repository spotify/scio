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

package com.spotify.scio.bigquery

import java.io.{File, FileInputStream}

import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.hadoop.util.ChainingHttpRequestInitializer
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.joda.time.format.PeriodFormatterBuilder
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe._
import scala.util.Try

/** A simple BigQuery client. */
// scalastyle:off number.of.methods
class BigQueryClient private(private val projectId: String,
                             _credentials: Credentials = null) {
  self =>

  require(projectId != null && projectId.nonEmpty, "Invalid projectId. " +
    "It should be a non-empty string")

  def this(projectId: String, secretFile: File) =
    this(
      projectId,
      GoogleCredentials
        .fromStream(new FileInputStream(secretFile))
        .createScoped(BigQueryClient.SCOPES))

  private lazy val credentials = Option(_credentials).getOrElse(
    GoogleCredentials.getApplicationDefault.createScoped(BigQueryClient.SCOPES))

  private lazy val bigquery: Bigquery = {
    val requestInitializer = new ChainingHttpRequestInitializer(
      new HttpCredentialsAdapter(credentials),
      new HttpRequestInitializer {
        override def initialize(request: HttpRequest): Unit = {
          BigQueryClient.connectTimeoutMs.foreach(request.setConnectTimeout)
          BigQueryClient.readTimeoutMs.foreach(request.setReadTimeout)
        }
      }
    )
    new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, requestInitializer)
      .setApplicationName("scio")
      .build()
  }

  private val logger = LoggerFactory.getLogger(this.getClass)

  private[bigquery] val PERIOD_FORMATTER = new PeriodFormatterBuilder()
    .appendHours().appendSuffix("h")
    .appendMinutes().appendSuffix("m")
    .appendSecondsWithOptionalMillis().appendSuffix("s")
    .toFormatter

  private[scio] def isCacheEnabled: Boolean = BigQueryClient.isCacheEnabled

  /* Tables services */
  private[scio] lazy val tables = new TableService(projectId, bigquery, credentials)

  /** Extract operations */
  lazy val extract: ExtractService = new ExtractService(projectId, bigquery)

  /** Load operations */
  lazy val load: LoadService = new LoadService(projectId, bigquery)

  /* Query operations */
  lazy private[scio] val queryService = new QueryService(projectId, bigquery, tables)


  /** Get schema for a query without executing it. */
  def getQuerySchema(sqlQuery: String): TableSchema = Cache.withCacheKey(sqlQuery) {
    queryService.getSchema(sqlQuery)
  }

  private[scio] def isLegacySql(sqlQuery: String, flattenResults: Boolean): Boolean =
    queryService.isLegacySql(sqlQuery, flattenResults)

  private[scio] def newQueryJob(sqlQuery: String, flattenResults: Boolean): QueryJob =
    queryService.newQueryJob(sqlQuery, flattenResults)

  /** Get rows from a query. */
  def getQueryRows(sqlQuery: String, flattenResults: Boolean = false): Iterator[TableRow] = {
    queryService.getRows(sqlQuery, flattenResults)
  }

  /** Get rows from a table. */
  def getTableRows(tableSpec: String): Iterator[TableRow] =
    tables.getRows(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get rows from a table. */
  def getTableRows(table: TableReference): Iterator[TableRow] =
    tables.getRows(table)

  /** Get schema from a table. */
  def getTableSchema(tableSpec: String): TableSchema =
    tables.getSchema(tableSpec)

  /** Get schema from a table. */
  def getTableSchema(table: TableReference): TableSchema =
    Cache.withCacheKey(bq.BigQueryHelpers.toTableSpec(table)) {
      tables.getSchema(table)
    }

  /** Get table metadata. */
  def getTable(tableSpec: String): Table =
    tables.get(tableSpec)

  /** Get table metadata. */
  def getTable(table: TableReference): Table =
    tables.get(table)

  /** Get list of tables in a dataset. */
  def getTables(projectId: String, datasetId: String): Seq[TableReference] =
    tables.get(projectId = projectId, datasetId = datasetId)

  /**
   * Check if table exists. Returns `true` if table exists, `false` is table definitely does not
   * exist, throws in other cases (BigQuery exception, network issue etc.).
   */
  def tableExists(table: TableReference): Boolean =
    tables.exists(table)

  /**
   * Check if table exists. Returns `true` if table exists, `false` is table definitely does not
   * exist, throws in other cases (BigQuery exception, network issue etc.).
   */
  def tableExists(tableSpec: String): Boolean =
    tables.exists(tableSpec)

  /**
   * Make a query and save results to a destination table.
   *
   * A temporary table will be created if `destinationTable` is `null` and a cached table will be
   * returned instead if one exists.
   */
  def query(sqlQuery: String,
            destinationTable: String = null,
            flattenResults: Boolean = false): TableReference =
    queryService.run(sqlQuery = sqlQuery,
      destinationTable = destinationTable,
      flattenResults = flattenResults
    )

  /** Write rows to a table. */
  def writeTableRows(table: TableReference, rows: List[TableRow], schema: TableSchema,
                     writeDisposition: WriteDisposition,
                     createDisposition: CreateDisposition): Unit =
    tables.writeRows(table, rows, schema, writeDisposition, createDisposition)

  /** Write rows to a table. */
  def writeTableRows(tableSpec: String, rows: List[TableRow], schema: TableSchema = null,
                     writeDisposition: WriteDisposition = WRITE_EMPTY,
                     createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit =
    tables.writeRows(
      bq.BigQueryHelpers.parseTableSpec(tableSpec),
      rows, schema, writeDisposition, createDisposition)

  /** Wait for all jobs to finish. */
  def waitForJobs(jobs: BigQueryJob*): Unit =
    JobService.waitForJobs(projectId, bigquery, jobs: _*)

  /** Delete table */
  private[bigquery] def deleteTable(table: TableReference): Unit =
    tables.delete(table)

  // =======================================================================
  // Type safe API
  // =======================================================================

  /**
   * Get a typed iterator for a BigQuery SELECT query or table.
   *
   * Note that `T` must be annotated with [[BigQueryType.fromSchema]],
   * [[BigQueryType.fromTable]], [[BigQueryType.fromQuery]], or [[BigQueryType.toTable]].
   *
   * By default the source (table or query) specified in the annotation will be used, but it can
   * be overridden with the `newSource` parameter. For example:
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   *
   * // Read from [publicdata:samples.gsod] as specified in the annotation.
   * bq.getTypedRows[Row]()
   *
   * // Read from [myproject:samples.gsod] instead.
   * bq.getTypedRows[Row]("myproject:samples.gsod")
   *
   * // Read from a query instead.
   * bq.getTypedRows[Row]("SELECT * FROM [publicdata:samples.gsod] LIMIT 1000")
   * }}}
   */
  def getTypedRows[T <: HasAnnotation : TypeTag](newSource: String = null)
  : Iterator[T] = {
    val bqt = BigQueryType[T]
    val rows = if (newSource == null) {
      // newSource is missing, T's companion object must have either table or query
      if (bqt.isTable) {
        self.getTableRows(bqt.table.get)
      } else if (bqt.isQuery) {
        self.getQueryRows(bqt.query.get)
      } else {
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      }
    } else {
      // newSource can be either table or query
      val table = scala.util.Try(bq.BigQueryHelpers.parseTableSpec(newSource)).toOption
      if (table.isDefined) {
        self.getTableRows(table.get)
      } else {
        self.getQueryRows(newSource)
      }
    }
    rows.map(bqt.fromTableRow)
  }

  /**
   * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
   * [[BigQueryType]].
   */
  def writeTypedRows[T <: HasAnnotation : TypeTag]
  (table: TableReference, rows: List[T],
   writeDisposition: WriteDisposition,
   createDisposition: CreateDisposition): Unit = {
    val bqt = BigQueryType[T]
    self.writeTableRows(
      table, rows.map(bqt.toTableRow), bqt.schema,
      writeDisposition, createDisposition)
  }

  /**
   * Write a List of rows to a BigQuery table. Note that element type `T` must be annotated with
   * [[BigQueryType]].
   */
  def writeTypedRows[T <: HasAnnotation : TypeTag]
  (tableSpec: String, rows: List[T],
   writeDisposition: WriteDisposition = WRITE_EMPTY,
   createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit =
    writeTypedRows(
      bq.BigQueryHelpers.parseTableSpec(tableSpec), rows,
      writeDisposition, createDisposition)

  // scalastyle:on parameter.number object.name

  private[bigquery] def temporaryTable(location: String): TableReference =
    tables.createTemporary(location)

  // =======================================================================
  // Query handling
  // =======================================================================


  /** Extract tables to be accessed by a query. */
  def extractTables(sqlQuery: String): Set[TableReference] =
    queryService.extractTables(sqlQuery)

  /** Extract locations of tables to be access by a query. */
  def extractLocation(sqlQuery: String): Option[String] =
    queryService.extractLocation(sqlQuery)

}

private[scio] object Cache {

  private[scio] def isCacheEnabled: Boolean = BigQueryClient.isCacheEnabled

  private[bigquery] def withCacheKey(key: String)(method: => TableSchema): TableSchema =
    if (isCacheEnabled) {
      getCacheSchema(key) match {
        case Some(schema) => schema
        case None =>
          val schema = method
          setCacheSchema(key, schema)
          schema
      }
    } else {
      method
    }

  private def setCacheSchema(key: String, schema: TableSchema): Unit =
    Files.write(schema.toPrettyString, schemaCacheFile(key), Charsets.UTF_8)

  private def getCacheSchema(key: String): Option[TableSchema] = Try {
    BigQueryUtil.parseSchema(scala.io.Source.fromFile(schemaCacheFile(key)).mkString)
  }.toOption

  private[bigquery] def setCacheDestinationTable(key: String, table: TableReference): Unit =
    Files.write(bq.BigQueryHelpers.toTableSpec(table), tableCacheFile(key), Charsets.UTF_8)

  private[bigquery] def getCacheDestinationTable(key: String): Option[TableReference] = Try {
    bq.BigQueryHelpers.parseTableSpec(scala.io.Source.fromFile(tableCacheFile(key)).mkString)
  }.toOption

  private def cacheFile(key: String, suffix: String): File = {
    val cacheDir = BigQueryClient.cacheDirectory
    val filename = Hashing.murmur3_128().hashString(key, Charsets.UTF_8).toString + suffix
    val cacheFile = new File(s"$cacheDir/$filename")
    Files.createParentDirs(cacheFile)
    cacheFile
  }

  private def schemaCacheFile(key: String): File = cacheFile(key, ".schema.json")

  private def tableCacheFile(key: String): File = cacheFile(key, ".table.txt")
}

// scalastyle:on number.of.methods

/** Companion object for [[BigQueryClient]]. */
object BigQueryClient {

  /** System property key for billing project. */
  val PROJECT_KEY: String = "bigquery.project"

  /** System property key for JSON secret path. */
  val SECRET_KEY: String = "bigquery.secret"

  /** System property key for local schema cache directory. */
  val CACHE_DIRECTORY_KEY: String = "bigquery.cache.directory"

  /** Default cache directory. */
  val CACHE_DIRECTORY_DEFAULT: String = sys.props("user.dir") + "/.bigquery"

  /** System property key for enabling or disabling scio bigquery caching */
  val CACHE_ENABLED_KEY: String = "bigquery.cache.enabled"

  /** Default cache behavior is enabled. */
  val CACHE_ENABLED_DEFAULT: Boolean = true

  /** System property key for priority, "BATCH" or "INTERACTIVE". */
  val PRIORITY_KEY = "bigquery.priority"

  /**
   * System property key for timeout in milliseconds to establish a connection.
   * Default is 20000 (20 seconds). 0 for an infinite timeout.
   */
  val CONNECT_TIMEOUT_MS_KEY: String = "bigquery.connect_timeout"

  /**
   * System property key for timeout in milliseconds to read data from an established connection.
   * Default is 20000 (20 seconds). 0 for an infinite timeout.
   */
  val READ_TIMEOUT_MS_KEY: String = "bigquery.read_timeout"

  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava

  private lazy val instance: BigQueryClient =
    if (sys.props(PROJECT_KEY) != null) {
      BigQueryClient(sys.props(PROJECT_KEY))
    } else {
      val project = new DefaultProjectFactory().create(null)
      if (project != null) {
        BigQueryClient(project)
      } else {
        throw new RuntimeException(
          s"Property $PROJECT_KEY not set. Use -D$PROJECT_KEY=<BILLING_PROJECT>")
      }
    }

  /**
   * Get the default BigQueryClient instance.
   *
   * Project must be set via `bigquery.project` system property.
   * An optional JSON secret file can be set via `bigquery.secret`.
   * For example, by adding the following code at the beginning of a job:
   * {{{
   * sys.props("bigquery.project") = "my-project"
   * sys.props("bigquery.secret") = "/path/to/secret.json"
   * }}}
   *
   * Or by passing them as SBT command line arguments:
   * {{{
   * sbt -Dbigquery.project=my-project -Dbigquery.secret=/path/to/secret.json
   * }}}
   */
  def defaultInstance(): BigQueryClient = instance

  /** Create a new BigQueryClient instance with the given project. */
  def apply(project: String): BigQueryClient = {
    val secret = sys.props(SECRET_KEY)
    if (secret == null) {
      new BigQueryClient(project)
    } else {
      BigQueryClient(project, new File(secret))
    }
  }

  /** Create a new BigQueryClient instance with the given project and credential. */
  def apply(project: String, credentials: Credentials): BigQueryClient =
    new BigQueryClient(project, credentials)

  /** Create a new BigQueryClient instance with the given project and secret file. */
  def apply(project: String, secretFile: File): BigQueryClient =
    new BigQueryClient(project, secretFile)

  /* caching config */
  private[bigquery] def isCacheEnabled: Boolean = Option(sys.props(CACHE_ENABLED_KEY))
    .flatMap(x => Try(x.toBoolean).toOption).getOrElse(CACHE_ENABLED_DEFAULT)

  private[bigquery] def cacheDirectory: String =
    getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT)

  /* bigquery config */
  private def connectTimeoutMs: Option[Int] = Option(sys.props(CONNECT_TIMEOUT_MS_KEY)).map(_.toInt)

  private def readTimeoutMs: Option[Int] = Option(sys.props(READ_TIMEOUT_MS_KEY)).map(_.toInt)

  /* query job config */
  private[bigquery] def priority: Option[String] = Option(sys.props(PRIORITY_KEY))

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
