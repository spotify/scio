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

import java.io.{File, FileInputStream, StringReader}
import java.util.UUID
import java.util.regex.Pattern

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition._
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition._
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import com.google.cloud.dataflow.sdk.options.GcpOptions.DefaultProjectFactory
import com.google.cloud.dataflow.sdk.util.{BigQueryTableInserter, BigQueryTableRowIterator}
import com.google.cloud.hadoop.util.ApiErrorExtractor
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.joda.time.format.{DateTimeFormat, PeriodFormatterBuilder}
import org.joda.time.{Instant, Period}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.{Map => MMap}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

/** Utility for BigQuery data types. */
object BigQueryUtil {

  // Ported from com.google.cloud.dataflow.sdk.io.BigQueryIO

  private val PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]"
  private val DATASET_REGEXP = "[-\\w.]{1,1024}"
  private val TABLE_REGEXP = "[-\\w$@]{1,1024}"
  private val DATASET_TABLE_REGEXP =
    s"((?<PROJECT>$PROJECT_ID_REGEXP):)?(?<DATASET>$DATASET_REGEXP)\\.(?<TABLE>$TABLE_REGEXP)"
  private val QUERY_TABLE_SPEC = Pattern.compile(s"(?<=\\[)$DATASET_TABLE_REGEXP(?=\\])")

  /** Parse a schema string. */
  def parseSchema(schemaString: String): TableSchema =
    new JsonObjectParser(new JacksonFactory)
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

}

/** A query job that may delay execution. */
private[scio] trait QueryJob {
  def waitForResult(): Unit
  val jobReference: Option[JobReference]
  val query: String
  val table: TableReference
}

/** A simple BigQuery client. */
// scalastyle:off number.of.methods
class BigQueryClient private (private val projectId: String,
                              credential: Credential = null) { self =>

  def this(projectId: String, secretFile: File) =
    this(
      projectId,
      GoogleCredential
        .fromStream(new FileInputStream(secretFile))
        .createScoped(BigQueryClient.SCOPES))

  private lazy val bigquery: Bigquery = {
    val c = Option(credential).getOrElse(
      GoogleCredential.getApplicationDefault.createScoped(BigQueryClient.SCOPES))
    val requestInitializer = new HttpRequestInitializer {
      override def initialize(request: HttpRequest): Unit = {
        BigQueryClient.connectTimeoutMs.foreach(request.setConnectTimeout)
        BigQueryClient.readTimeoutMs.foreach(request.setReadTimeout)
        // Credential also implements HttpRequestInitializer
        c.initialize(request)
      }
    }
    new Bigquery.Builder(new NetHttpTransport, new JacksonFactory, c)
      .setHttpRequestInitializer(requestInitializer)
      .setApplicationName("scio")
      .build()
  }

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryClient])

  private val TABLE_PREFIX = "scio_query"
  private val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  private val PERIOD_FORMATTER = new PeriodFormatterBuilder()
    .appendHours().appendSuffix("h")
    .appendMinutes().appendSuffix("m")
    .appendSecondsWithOptionalMillis().appendSuffix("s")
    .toFormatter

  private val STAGING_DATASET_PREFIX = "scio_bigquery_staging_"
  private val STAGING_DATASET_TABLE_EXPIRATION_MS = 86400000L
  private val STAGING_DATASET_DESCRIPTION = "Staging dataset for temporary tables"

  private def isInteractive =
    Thread
      .currentThread()
      .getStackTrace
      .exists { e =>
        e.getClassName.startsWith("scala.tools.nsc.interpreter.") ||
          e.getClassName.startsWith("org.scalatest.tools.")
      }

  private val PRIORITY = if (isInteractive) "INTERACTIVE" else "BATCH"

  /** Get schema for a query without executing it. */
  def getQuerySchema(sqlQuery: String): TableSchema = withCacheKey(sqlQuery) {
    if (isLegacySql(sqlQuery, flattenResults = false)) {
      // Dry-run not supported for legacy query, using view as a work around
      logger.info("Getting legacy query schema with view")
      val location = extractLocation(sqlQuery)
      prepareStagingDataset(location)
      val temp = temporaryTable(location)

      // Create temporary table view and get schema
      logger.info(s"Creating temporary view ${BigQueryIO.toTableSpec(temp)}")
      val view = new ViewDefinition().setQuery(sqlQuery)
      val viewTable = new Table().setView(view).setTableReference(temp)
      val schema = bigquery
        .tables().insert(temp.getProjectId, temp.getDatasetId, viewTable)
        .execute().getSchema

      // Delete temporary table
      logger.info(s"Deleting temporary view ${BigQueryIO.toTableSpec(temp)}")
      bigquery.tables().delete(temp.getProjectId, temp.getDatasetId, temp.getTableId).execute()

      schema
    } else {
      // Get query schema via dry-run
      logger.info("Getting SQL query schema with dry-run")
      runQuery(
        sqlQuery, null,
        flattenResults = false, useLegacySql = false, dryRun = true)
        .get.getStatistics.getQuery.getSchema
    }
  }

  /** Get rows from a query. */
  def getQueryRows(sqlQuery: String, flattenResults: Boolean = false): Iterator[TableRow] = {
    val queryJob = newQueryJob(sqlQuery, flattenResults)
    queryJob.waitForResult()
    getTableRows(queryJob.table)
  }

  /** Get rows from a table. */
  def getTableRows(tableSpec: String): Iterator[TableRow] =
    getTableRows(BigQueryIO.parseTableSpec(tableSpec))

  /** Get rows from a table. */
  def getTableRows(table: TableReference): Iterator[TableRow] = new Iterator[TableRow] {
    private val iterator = BigQueryTableRowIterator.fromTable(table, bigquery)
    private var _isOpen = false
    private var _hasNext = false
    private def init(): Unit = if (!_isOpen) {
      iterator.open()
      _isOpen = true
      _hasNext = iterator.advance()
    }
    override def hasNext: Boolean = {
      init()
      _hasNext
    }
    override def next(): TableRow = {
      init()
      if (_hasNext) {
        val r = iterator.getCurrent
        _hasNext = iterator.advance()
        r
      } else {
        throw new NoSuchElementException
      }
    }
  }

  /** Get schema from a table. */
  def getTableSchema(tableSpec: String): TableSchema =
    getTableSchema(BigQueryIO.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def getTableSchema(table: TableReference): TableSchema =
    withCacheKey(BigQueryIO.toTableSpec(table)) {
      getTable(table).getSchema
    }

  /** Get table metadata **/
  def getTable(tableSpec: String): Table =
    getTable(BigQueryIO.parseTableSpec(tableSpec))

  /** Get table metadata **/
  def getTable(table: TableReference): Table = {
    val p = if (table.getProjectId == null) this.projectId else table.getProjectId
    bigquery.tables().get(p, table.getDatasetId, table.getTableId).execute()
  }

  /**
   * Make a query and save results to a destination table.
   *
   * A temporary table will be created if `destinationTable` is `null` and a cached table will be
   * returned instead if one exists.
   */
  def query(sqlQuery: String,
            destinationTable: String = null,
            flattenResults: Boolean = false): TableReference =
    if (destinationTable != null) {
      val tableRef = BigQueryIO.parseTableSpec(destinationTable)
      val queryJob = delayedQueryJob(sqlQuery, tableRef, flattenResults)
      queryJob.waitForResult()
      tableRef
    } else {
      val queryJob = newQueryJob(sqlQuery, flattenResults)
      queryJob.waitForResult()
      queryJob.table
    }

  /** Write rows to a table. */
  def writeTableRows(table: TableReference, rows: List[TableRow], schema: TableSchema,
                     writeDisposition: WriteDisposition,
                     createDisposition: CreateDisposition): Unit = {
    val inserter = new BigQueryTableInserter(bigquery)
    inserter.getOrCreateTable(table, writeDisposition, createDisposition, schema)
    inserter.insertAll(table, rows.asJava)
  }

  /** Write rows to a table. */
  def writeTableRows(tableSpec: String, rows: List[TableRow], schema: TableSchema = null,
                     writeDisposition: WriteDisposition = WRITE_EMPTY,
                     createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit =
    writeTableRows(
      BigQueryIO.parseTableSpec(tableSpec), rows, schema, writeDisposition, createDisposition)

  /** Wait for all jobs to finish. */
  def waitForJobs(jobs: QueryJob*): Unit = {
    val numTotal = jobs.size
    var pendingJobs = jobs.filter(_.jobReference.isDefined)

    while (pendingJobs.nonEmpty) {
      val remainingJobs = pendingJobs.filter { j =>
        val jobId = j.jobReference.get.getJobId
        val poll = bigquery.jobs().get(projectId, jobId).execute()
        val error = poll.getStatus.getErrorResult
        if (error != null) {
          throw new RuntimeException(s"Query job failed: id: $jobId, error: $error")
        }
        if (poll.getStatus.getState == "DONE") {
          logJobStatistics(j.query, poll)
          false
        } else {
          true
        }
      }

      pendingJobs = remainingJobs
      val numDone = numTotal - pendingJobs.size
      logger.info(s"Query: $numDone out of $numTotal completed")
      if (pendingJobs.nonEmpty) {
        Thread.sleep(10000)
      }
    }
  }

  // =======================================================================
  // Job execution
  // =======================================================================

  private[scio] def newQueryJob(sqlQuery: String, flattenResults: Boolean): QueryJob = {
    try {
      val sourceTimes = extractTables(sqlQuery).map(t => BigInt(getTable(t).getLastModifiedTime))
      val temp = getCacheDestinationTable(sqlQuery).get
      val time = BigInt(getTable(temp).getLastModifiedTime)
      if (sourceTimes.forall(_ < time)) {
        logger.info(s"Cache hit for query: `$sqlQuery`")
        logger.info(s"Existing destination table: ${BigQueryIO.toTableSpec(temp)}")
        new QueryJob {
          override def waitForResult(): Unit = {}
          override val jobReference: Option[JobReference] = None
          override val query: String = sqlQuery
          override val table: TableReference = temp
        }
      } else {
        logger.info(s"Cache invalid for query: `$sqlQuery`")
        logger.info(s"New destination table: ${BigQueryIO.toTableSpec(temp)}")
        setCacheDestinationTable(sqlQuery, temp)
        delayedQueryJob(sqlQuery, temp, flattenResults)
      }
    } catch {
      case NonFatal(_) =>
        val temp = temporaryTable(extractLocation(sqlQuery))
        logger.info(s"Cache miss for query: `$sqlQuery`")
        logger.info(s"New destination table: ${BigQueryIO.toTableSpec(temp)}")
        setCacheDestinationTable(sqlQuery, temp)
        delayedQueryJob(sqlQuery, temp, flattenResults)
    }
  }

  private def prepareStagingDataset(location: String): Unit = {
    val datasetId = STAGING_DATASET_PREFIX + location.toLowerCase
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if new ApiErrorExtractor().itemNotFound(e) =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val dsRef = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(STAGING_DATASET_TABLE_EXPIRATION_MS)
          .setDescription(STAGING_DATASET_DESCRIPTION)
          .setLocation(location)
        bigquery
          .datasets()
          .insert(projectId, ds)
          .execute()
      case NonFatal(e) => throw e
    }
  }

  private def temporaryTable(location: String): TableReference = {
    val now = Instant.now().toString(TIME_FORMATTER)
    val tableId = TABLE_PREFIX + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(STAGING_DATASET_PREFIX + location.toLowerCase)
      .setTableId(tableId)
  }

  private def delayedQueryJob(sqlQuery: String,
                              destinationTable: TableReference,
                              flattenResults: Boolean): QueryJob = new QueryJob {
    override def waitForResult(): Unit = self.waitForJobs(this)
    override lazy val jobReference: Option[JobReference] = {
      val location = extractLocation(sqlQuery)
      prepareStagingDataset(location)
      val isLegacy = isLegacySql(sqlQuery, flattenResults)
      if (isLegacy) {
        logger.info(s"Executing legacy query: `$sqlQuery`")
      } else {
        logger.info(s"Executing SQL query: `$sqlQuery`")
      }
      val tryRun = runQuery(sqlQuery, destinationTable, flattenResults, isLegacy, dryRun = false)
      Some(tryRun.get.getJobReference)
    }
    override val query: String = sqlQuery
    override val table: TableReference = destinationTable
  }

  private def logJobStatistics(sqlQuery: String, job: Job): Unit = {
    val jobId = job.getJobReference.getJobId
    val stats = job.getStatistics
    logger.info(s"Query completed: jobId: $jobId")
    logger.info(s"Query: `$sqlQuery`")

    val elapsed = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getCreationTime))
    val pending = PERIOD_FORMATTER.print(new Period(stats.getStartTime - stats.getCreationTime))
    val execution = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getStartTime))
    logger.info(s"Elapsed: $elapsed, pending: $pending, execution: $execution")

    val bytes = FileUtils.byteCountToDisplaySize(stats.getQuery.getTotalBytesProcessed)
    val cacheHit = stats.getQuery.getCacheHit
    logger.info(s"Total bytes processed: $bytes, cache hit: $cacheHit")
  }

  // =======================================================================
  // Query handling
  // =======================================================================

  private val dryRunCache: MMap[(String, Boolean, Boolean), Try[Job]] = MMap.empty

  private def runQuery(sqlQuery: String,
                       destinationTable: TableReference,
                       flattenResults: Boolean,
                       useLegacySql: Boolean,
                       dryRun: Boolean): Try[Job] = {
    def run = Try {
      val queryConfig = new JobConfigurationQuery()
        .setQuery(sqlQuery)
        .setUseLegacySql(useLegacySql)
        .setFlattenResults(flattenResults)
        .setPriority(PRIORITY)
        .setCreateDisposition("CREATE_IF_NEEDED")
        .setWriteDisposition("WRITE_EMPTY")
      if (!dryRun) {
        queryConfig.setAllowLargeResults(true).setDestinationTable(destinationTable)
      }
      val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(dryRun)
      val fullJobId = projectId + "-" + UUID.randomUUID().toString
      val jobReference = new JobReference().setProjectId(projectId).setJobId(fullJobId)
      val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
      bigquery.jobs().insert(projectId, job).execute()
    }

    if (dryRun) {
      dryRunCache.getOrElseUpdate((sqlQuery, flattenResults, useLegacySql), run)
    } else {
      run
    }
  }

  private def isLegacySql(sqlQuery: String, flattenResults: Boolean): Boolean = {
    def isInvalidQuery(e: GoogleJsonResponseException): Boolean =
      e.getDetails.getErrors.get(0).getReason == "invalidQuery"
    def dryRunQuery(useLegacySql: Boolean): Try[Job] =
      runQuery(sqlQuery, null, flattenResults, useLegacySql, dryRun = true)

    // dry run with SQL syntax first
    dryRunQuery(false) match {
      case Success(_) => false
      case Failure(e: GoogleJsonResponseException) if isInvalidQuery(e) =>
        // dry run with legacy syntax next
        dryRunQuery(true) match {
          case Success(_) =>
            logger.warn("Legacy syntax is deprecated, use SQL syntax instead. " +
              "See https://cloud.google.com/bigquery/sql-reference/")
            logger.warn(s"Legacy query: `$sqlQuery`")
            true
          case Failure(f) => throw f
        }
      case Failure(e) => throw e
    }
  }

  /** Extract tables to be accessed by a query. */
  def extractTables(sqlQuery: String): Set[TableReference] = {
    val isLegacy = isLegacySql(sqlQuery, flattenResults = false)
    val tryJob = runQuery(sqlQuery, null, flattenResults = false, isLegacy, dryRun = true)
    tryJob.get.getStatistics.getQuery.getReferencedTables.asScala.toSet
  }

  /** Extract locations of tables to be access by a query. */
  def extractLocation(sqlQuery: String): String = {
    val locations = extractTables(sqlQuery)
      .map(t => (t.getProjectId, t.getDatasetId))
      .map { case (pId, dId) =>
        val l = bigquery.datasets().get(pId, dId).execute().getLocation
        if (l != null) l else "US"
      }
    require(locations.size == 1, "Tables in the query must be in the same location")
    locations.head
  }

  // =======================================================================
  // Schema and query caching
  // =======================================================================

  private def withCacheKey(key: String)
                          (method: => TableSchema): TableSchema = getCacheSchema(key) match {
    case Some(schema) => schema
    case None =>
      val schema = method
      setCacheSchema(key, schema)
      schema
  }

  private def setCacheSchema(key: String, schema: TableSchema): Unit =
    Files.write(schema.toPrettyString, schemaCacheFile(key), Charsets.UTF_8)

  private def getCacheSchema(key: String): Option[TableSchema] = Try {
    BigQueryUtil.parseSchema(scala.io.Source.fromFile(schemaCacheFile(key)).mkString)
  }.toOption

  private def setCacheDestinationTable(key: String, table: TableReference): Unit =
    Files.write(BigQueryIO.toTableSpec(table), tableCacheFile(key), Charsets.UTF_8)

  private def getCacheDestinationTable(key: String): Option[TableReference] = Try {
    BigQueryIO.parseTableSpec(scala.io.Source.fromFile(tableCacheFile(key)).mkString)
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

  private var instance: BigQueryClient = null

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
  def defaultInstance(): BigQueryClient = {
    if (instance == null) {
      instance = if (sys.props(PROJECT_KEY) != null) {
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
    }
    instance
  }

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
  def apply(project: String, credential: Credential): BigQueryClient =
    new BigQueryClient(project, credential)

  /** Create a new BigQueryClient instance with the given project and secret file. */
  def apply(project: String, secretFile: File): BigQueryClient =
    new BigQueryClient(project, secretFile)

  private def cacheDirectory: String = getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT)

  private def connectTimeoutMs: Option[Int] = Option(sys.props(CONNECT_TIMEOUT_MS_KEY)).map(_.toInt)

  private def readTimeoutMs: Option[Int] = Option(sys.props(READ_TIMEOUT_MS_KEY)).map(_.toInt)

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
