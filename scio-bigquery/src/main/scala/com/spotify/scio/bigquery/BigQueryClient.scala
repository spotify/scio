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

import java.io.{File, FileInputStream, InputStream, StringReader}
import java.util.UUID

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition._
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition._
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import com.google.cloud.dataflow.sdk.util.{BigQueryTableInserter, BigQueryTableRowIterator}
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.joda.time.format.{DateTimeFormat, PeriodFormatterBuilder}
import org.joda.time.{Instant, Period}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal
import scala.util.{Random, Try}

/** Utility for BigQuery data types. */
object BigQueryUtil {

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

// scalastyle:off number.of.methods
/** A simple BigQuery client. */
class BigQueryClient private (private val projectId: String,
                              credential: Credential = null) { self =>

  def this(projectId: String, secretStream: InputStream) =
    this(projectId, GoogleCredential.fromStream(secretStream).createScoped(BigQueryClient.SCOPES))

  def this(projectId: String, secretFile: File) = this(projectId, new FileInputStream(secretFile))

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
  private val JOB_ID_PREFIX = "scio_query"
  private val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  private val PERIOD_FORMATTER = new PeriodFormatterBuilder()
    .appendHours().appendSuffix("h")
    .appendMinutes().appendSuffix("m")
    .appendSecondsWithOptionalMillis().appendSuffix("s")
    .toFormatter

  private def inConsole =
    Thread
      .currentThread()
      .getStackTrace
      .exists(_.getClassName.startsWith("scala.tools.nsc.interpreter."))

  private val PRIORITY = if (inConsole) "INTERACTIVE" else "BATCH"

  /** Get tables referenced in a query. */
  def getQueryTables(sqlQuery: String, flattenResults: Boolean = false): Seq[TableReference] = {
    val job = makeQuery(sqlQuery, null, flattenResults, dryRun = true)
    job.getStatistics.getQuery.getReferencedTables.asScala
  }

  /** Get schema for a query without executing it. */
  def getQuerySchema(sqlQuery: String,
                     flattenResults: Boolean = false): TableSchema = withCacheKey(sqlQuery) {
    val job = makeQuery(sqlQuery, null, flattenResults, dryRun = true)
    job.getStatistics.getQuery.getSchema
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
      val sourceTimes = getQueryTables(sqlQuery).map(t => BigInt(getTable(t).getLastModifiedTime))
      val temp = getCacheDestinationTable(sqlQuery).get
      val time = BigInt(getTable(temp).getLastModifiedTime)
      if (sourceTimes.forall(_ < time)) {
        logger.info(s"Cache hit for query: $sqlQuery")
        logger.info(s"Existing destination table: ${BigQueryIO.toTableSpec(temp)}")
        new QueryJob {
          override def waitForResult(): Unit = {}
          override val jobReference: Option[JobReference] = None
          override val query: String = sqlQuery
          override val table: TableReference = temp
        }
      } else {
        val temp = temporaryTable()
        logger.info(s"Cache invalid for query: $sqlQuery")
        logger.info(s"New destination table: ${BigQueryIO.toTableSpec(temp)}")
        setCacheDestinationTable(sqlQuery, temp)
        delayedQueryJob(sqlQuery, temp, flattenResults)
      }
    } catch {
      case NonFatal(_) =>
        val temp = temporaryTable()
        logger.info(s"Cache miss for query: $sqlQuery")
        logger.info(s"New destination table: ${BigQueryIO.toTableSpec(temp)}")
        setCacheDestinationTable(sqlQuery, temp)
        delayedQueryJob(sqlQuery, temp, flattenResults)
    }
  }

  private def prepareStagingDataset(): Unit = {
    // Create staging dataset if it does not already exist
    val datasetId = BigQueryClient.stagingDataset
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val dsRef = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(BigQueryClient.STAGING_DATASET_TABLE_EXPIRATION_MS)
          .setDescription(BigQueryClient.STAGING_DATASET_DESCRIPTION)
          .setLocation(BigQueryClient.stagingDatasetLocation)
        bigquery
          .datasets()
          .insert(projectId, ds)
          .execute()
      case NonFatal(e) => throw e
    }
  }

  private def temporaryTable(): TableReference = {
    val now = Instant.now().toString(TIME_FORMATTER)
    val tableId = TABLE_PREFIX + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(BigQueryClient.stagingDataset)
      .setTableId(tableId)
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

  private def delayedQueryJob(sqlQuery: String,
                              destinationTable: TableReference,
                              flattenResults: Boolean): QueryJob = new QueryJob {
    override def waitForResult(): Unit = self.waitForJobs(this)
    override lazy val jobReference: Option[JobReference] = {
      prepareStagingDataset()
      logger.info(s"Executing query: $sqlQuery")
      Some(makeQuery(sqlQuery, destinationTable, flattenResults, dryRun = false).getJobReference)
    }
    override val query: String = sqlQuery
    override val table: TableReference = destinationTable
  }

  private def makeQuery(sqlQuery: String,
                        destinationTable: TableReference,
                        flattenResults: Boolean,
                        dryRun: Boolean): Job = {
    var queryConfig: JobConfigurationQuery = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setFlattenResults(flattenResults)
      .setPriority(PRIORITY)
      .setUseLegacySql(false)
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")
    if (destinationTable != null) {
      queryConfig = queryConfig
        .setAllowLargeResults(true)
        .setDestinationTable(destinationTable)
    }

    val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(dryRun)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute()
  }

  private def logJobStatistics(sqlQuery: String, job: Job): Unit = {
    val jobId = job.getJobReference.getJobId
    val stats = job.getStatistics
    logger.info(s"Query completed: jobId: $jobId")
    logger.info(s"Query: $sqlQuery")

    val elapsed = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getCreationTime))
    val pending = PERIOD_FORMATTER.print(new Period(stats.getStartTime - stats.getCreationTime))
    val execution = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getStartTime))
    logger.info(s"Elapsed: $elapsed, pending: $pending, execution: $execution")

    val bytes = FileUtils.byteCountToDisplaySize(stats.getQuery.getTotalBytesProcessed)
    val cacheHit = stats.getQuery.getCacheHit
    logger.info(s"Total bytes processed: $bytes, cache hit: $cacheHit")
  }

  private def getTable(table: TableReference): Table = {
    val p = if (table.getProjectId == null) this.projectId else table.getProjectId
    bigquery.tables().get(p, table.getDatasetId, table.getTableId).execute()
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
    val outputFile = new File(cacheDir)
    if (!outputFile.exists()) {
      outputFile.mkdirs()
    }
    val filename = Hashing.sha1().hashString(key, Charsets.UTF_8).toString.substring(0, 32) + suffix
    new File(s"$cacheDir/$filename")
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

  /** System property key for staging dataset. */
  val STAGING_DATASET_KEY: String = "bigquery.staging_dataset"

  /** Default staging dataset. */
  val STAGING_DATASET_DEFAULT: String = "scio_bigquery_staging"

  /** System property key for staging dataset location. */
  val STAGING_DATASET_LOCATION_KEY: String = "bigquery.staging_dataset.location"

  /** Default staging dataset location. */
  val STAGING_DATASET_LOCATION_DEFAULT: String = "US"

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

  /** Table expiration in milliseconds for staging dataset. */
  val STAGING_DATASET_TABLE_EXPIRATION_MS: Long = 86400000L

  /** Description for staging dataset. */
  val STAGING_DATASET_DESCRIPTION: String = "Staging dataset for temporary tables"

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
      val project = sys.props(PROJECT_KEY)
      if (project == null) {
        throw new RuntimeException(
          s"Property $PROJECT_KEY not set. Use -D$PROJECT_KEY=<BILLING_PROJECT>")
      }
      instance = BigQueryClient(project)
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

  private def stagingDataset: String =
    getPropOrElse(
      STAGING_DATASET_KEY,
      STAGING_DATASET_DEFAULT + "_" + stagingDatasetLocation.toLowerCase)

  // Location in create dataset request must be upper case, e.g. US, EU
  private def stagingDatasetLocation: String =
    getPropOrElse(STAGING_DATASET_LOCATION_KEY, STAGING_DATASET_LOCATION_DEFAULT).toUpperCase

  private def cacheDirectory: String =
    getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT) + "/" + scioVersion

  private def connectTimeoutMs: Option[Int] = Option(sys.props(CONNECT_TIMEOUT_MS_KEY)).map(_.toInt)

  private def readTimeoutMs: Option[Int] = Option(sys.props(READ_TIMEOUT_MS_KEY)).map(_.toInt)

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

  /** Get Scio version from scio-bigquery/src/main/resources/version.sbt. */
  private def scioVersion: String = {
    val stream = this.getClass.getResourceAsStream("/version.sbt")
    val line = scala.io.Source.fromInputStream(stream).getLines().next()
    """version in .+"([^"]+)"""".r.findFirstMatchIn(line).get.group(1)
  }

}
