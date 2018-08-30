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

import java.io.{File, FileInputStream, IOException, StringReader}
import java.util.UUID

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.http.{HttpRequest, HttpRequestInitializer}
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.model._
import com.google.api.services.bigquery.{Bigquery, BigqueryScopes}
import com.google.auth.Credentials
import com.google.auth.http.HttpCredentialsAdapter
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.hadoop.util.{ApiErrorExtractor, ChainingHttpRequestInitializer}
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions.DefaultProjectFactory
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.commons.io.FileUtils
import org.joda.time.format.{DateTimeFormat, PeriodFormatterBuilder}
import org.joda.time.{Instant, Period}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.reflect.runtime.universe._
import scala.util.control.NonFatal
import scala.util.{Failure, Random, Success, Try}

/** Utility for BigQuery data types. */
object BigQueryUtil {

  private lazy val jsonObjectParser = new JsonObjectParser(new JacksonFactory)

  /** Parse a schema string. */
  def parseSchema(schemaString: String): TableSchema =
    jsonObjectParser
      .parseAndClose(new StringReader(schemaString), classOf[TableSchema])

}

/** A BigQueryJob */
private[scio] sealed trait BigQueryJob {
  val jobReference: Option[JobReference]
  val jobType: String
  val table: TableReference
}

/* Extract Job Container */
private[scio] case class ExtractJob(destinationUris: List[String],
                                    jobReference: Option[JobReference],
                                    table: TableReference)
  extends BigQueryJob {

  val jobType = "Extract"
}

/* Load Job Container */
private[scio] case class LoadJob(sources: List[String],
                                 jobReference: Option[JobReference],
                                 table: TableReference)
  extends BigQueryJob {

  val jobType = "Load"
}

/** A query job that may delay execution. */
private[scio] trait QueryJob extends BigQueryJob {
  def waitForResult(): Unit
  val jobReference: Option[JobReference]
  val query: String
  val table: TableReference
  val jobType: String = "Query"
}

/** A simple BigQuery client. */
// scalastyle:off number.of.methods
// scalastyle:off file.size.limit
class BigQueryClient private (private val projectId: String,
                              _credentials: Credentials = null) { self =>

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

  private lazy val bqService = {
    val options = PipelineOptionsFactory.create().as(classOf[beam.BigQueryOptions])
    options.setProject(projectId)
    options.setGcpCredential(credentials)

    new beam.BigQueryServicesWrapper(options)
  }

  private val logger = LoggerFactory.getLogger(this.getClass)

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

  private val DEFAULT_LOCATION = "US"

  private def isInteractive =
    BigQueryClient.priority
      .map(_ == "INTERACTIVE")
      .getOrElse(
        Thread
          .currentThread()
          .getStackTrace
          .exists { e =>
            e.getClassName.startsWith("scala.tools.nsc.interpreter.") ||
            e.getClassName.startsWith("org.scalatest.tools.")
          }
      )

  private val PRIORITY = if (isInteractive) "INTERACTIVE" else "BATCH"

  private[scio] def isCacheEnabled: Boolean = BigQueryClient.isCacheEnabled

  /** Get schema for a query without executing it. */
  def getQuerySchema(sqlQuery: String): TableSchema = withCacheKey(sqlQuery) {
    if (isLegacySql(sqlQuery, flattenResults = false)) {
      // Dry-run not supported for legacy query, using view as a work around
      logger.info("Getting legacy query schema with view")
      val location = extractLocation(sqlQuery).getOrElse(DEFAULT_LOCATION)
      prepareStagingDataset(location)
      val temp = temporaryTable(location)

      // Create temporary table view and get schema
      logger.info(s"Creating temporary view ${beam.BigQueryHelpers.toTableSpec(temp)}")
      val view = new ViewDefinition().setQuery(sqlQuery)
      val viewTable = new Table().setView(view).setTableReference(temp)
      val schema = bigquery
        .tables().insert(temp.getProjectId, temp.getDatasetId, viewTable)
        .execute().getSchema

      // Delete temporary table
      logger.info(s"Deleting temporary view ${beam.BigQueryHelpers.toTableSpec(temp)}")
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
    getTableRows(beam.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get rows from a table. */
  def getTableRows(table: TableReference): Iterator[TableRow] = new Iterator[TableRow] {
    private val iterator = beam.PatchedBigQueryTableRowIterator.fromTable(table, bigquery)
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
    getTableSchema(beam.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def getTableSchema(table: TableReference): TableSchema =
    withCacheKey(beam.BigQueryHelpers.toTableSpec(table)) {
      getTable(table).getSchema
    }

  /** Get table metadata. */
  def getTable(tableSpec: String): Table =
    getTable(beam.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get table metadata. */
  def getTable(table: TableReference): Table = {
    val p = if (table.getProjectId == null) this.projectId else table.getProjectId
    bigquery.tables().get(p, table.getDatasetId, table.getTableId).execute()
  }

  /** Get list of tables in a dataset. */
  def getTables(projectId: String, datasetId: String): Seq[TableReference] = {
    val b = Seq.newBuilder[TableReference]
    val req = bigquery.tables().list(projectId, datasetId)
    var rep = req.execute()
    Option(rep.getTables).foreach(_.asScala.foreach(b += _.getTableReference))
    while (rep.getNextPageToken != null) {
      rep = req.setPageToken(rep.getNextPageToken).execute()
      Option(rep.getTables)
        .foreach(_.asScala.foreach(b += _.getTableReference))
    }
    b.result()
  }

  /**
   * Check if table exists. Returns `true` if table exists, `false` is table definitely does not
   * exist, throws in other cases (BigQuery exception, network issue etc.).
   */
  def tableExists(table: TableReference): Boolean = try {
    getTable(table)
    true
  } catch {
    case e: GoogleJsonResponseException if e.getDetails.getErrors.get(0).getReason == "notFound" =>
      false
    case e: Throwable => throw e
  }

  /**
   * Check if table exists. Returns `true` if table exists, `false` is table definitely does not
   * exist, throws in other cases (BigQuery exception, network issue etc.).
   */
  def tableExists(tableSpec: String): Boolean =
    tableExists(beam.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Deletes a table */
  private[bigquery] def deleteTable(table: TableReference): Unit = {
    bigquery.tables().delete(table.getProjectId, table.getDatasetId, table.getTableId)
      .execute()
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
      val tableRef = beam.BigQueryHelpers.parseTableSpec(destinationTable)
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
    try {
      if (createDisposition == CREATE_IF_NEEDED) {
        createTable(table, schema)
      }
      bqService.insertAll(table, rows.asJava)
    } finally {
      val options = PipelineOptionsFactory.create().as(classOf[GcsOptions])
      Option(options.getExecutorService).foreach(_.shutdown())
    }
  }

  /** Write rows to a table. */
  def writeTableRows(tableSpec: String, rows: List[TableRow], schema: TableSchema,
                     writeDisposition: WriteDisposition = WRITE_EMPTY,
                     createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit =
    writeTableRows(
      beam.BigQueryHelpers.parseTableSpec(tableSpec),
      rows, schema, writeDisposition, createDisposition)

  def createTable(table: Table): Unit =
    bqService.createTable(table)

  def createTable(table: TableReference, schema: TableSchema): Unit =
    createTable(new Table().setTableReference(table).setSchema(schema))

  def createTable(tableSpec: String, schema: TableSchema): Unit =
    createTable(beam.BigQueryHelpers.parseTableSpec(tableSpec), schema)

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
      val table = scala.util.Try(beam.BigQueryHelpers.parseTableSpec(newSource)).toOption
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
      beam.BigQueryHelpers.parseTableSpec(tableSpec), rows,
      writeDisposition, createDisposition)

  def createTypedTable[T <: HasAnnotation: TypeTag](table: Table): Unit =
    createTable(table.setSchema(BigQueryType[T].schema))

  def createTypedTable[T <: HasAnnotation: TypeTag](table: TableReference): Unit =
    createTable(table, BigQueryType[T].schema)

  def createTypedTable[T <: HasAnnotation: TypeTag](tableSpec: String): Unit =
    createTypedTable(beam.BigQueryHelpers.parseTableSpec(tableSpec))

  // =======================================================================
  // Job execution
  // =======================================================================

  private[scio] def newQueryJob(sqlQuery: String, flattenResults: Boolean): QueryJob = {
    if (isCacheEnabled) {
      newCachedQueryJob(sqlQuery, flattenResults)
    } else {
      logger.info(s"BigQuery caching is disabled")
      val tempTable = temporaryTable(extractLocation(sqlQuery).getOrElse(DEFAULT_LOCATION))
      delayedQueryJob(sqlQuery, tempTable, flattenResults)
    }
  }

  private[scio] def newCachedQueryJob(sqlQuery: String, flattenResults: Boolean): QueryJob = {
    try {
      val sourceTimes = extractTables(sqlQuery)
        .map(t => BigInt(getTable(t).getLastModifiedTime))
      val temp = getCacheDestinationTable(sqlQuery).get
      val time = BigInt(getTable(temp).getLastModifiedTime)
      if (sourceTimes.forall(_ < time)) {
        logger.info(s"Cache hit for query: `$sqlQuery`")
        logger.info(s"Existing destination table: ${beam.BigQueryHelpers.toTableSpec(temp)}")
        new QueryJob {
          override def waitForResult(): Unit = {}
          override val jobReference: Option[JobReference] = None
          override val query: String = sqlQuery
          override val table: TableReference = temp
        }
      } else {
        logger.info(s"Cache invalid for query: `$sqlQuery`")
        val newTemp = temporaryTable(extractLocation(sqlQuery).getOrElse(DEFAULT_LOCATION))
        logger.info(s"New destination table: ${beam.BigQueryHelpers.toTableSpec(newTemp)}")
        setCacheDestinationTable(sqlQuery, newTemp)
        delayedQueryJob(sqlQuery, newTemp, flattenResults)
      }
    } catch {
      case NonFatal(e: GoogleJsonResponseException) if isInvalidQuery(e) => throw e
      case NonFatal(_) =>
        val temp = temporaryTable(extractLocation(sqlQuery).getOrElse(DEFAULT_LOCATION))
        logger.info(s"Cache miss for query: `$sqlQuery`")
        logger.info(s"New destination table: ${beam.BigQueryHelpers.toTableSpec(temp)}")
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

  private[bigquery] def temporaryTable(location: String): TableReference = {
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
      val location = extractLocation(sqlQuery).getOrElse(DEFAULT_LOCATION)
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

  private def isInvalidQuery(e: GoogleJsonResponseException): Boolean =
    e.getDetails.getErrors.get(0).getReason == "invalidQuery"

  private[scio] def isLegacySql(sqlQuery: String, flattenResults: Boolean): Boolean = {
    def dryRunQuery(useLegacySql: Boolean): Try[Job] =
      runQuery(sqlQuery, null, flattenResults, useLegacySql, dryRun = true)

    sqlQuery.trim.split("\n")(0).trim.toLowerCase match {
      case "#legacysql" => true
      case "#standardsql" => false
      case _ =>

        // dry run with SQL syntax first
        dryRunQuery(false) match {
          case Success(_) => false
          case Failure(e: GoogleJsonResponseException) if isInvalidQuery(e) =>
            // dry run with legacy syntax next
            dryRunQuery(true) match {
              case Success(_) =>
                logger.warn("Legacy syntax is deprecated, use SQL syntax instead. " +
                  "See https://cloud.google.com/bigquery/docs/reference/standard-sql/")
                logger.warn(s"Legacy query: `$sqlQuery`")
                true
              case Failure(f) =>
                logger.error(
                  s"Tried both standard and legacy syntax, query `$sqlQuery` failed for both!")
                logger.error("Standard syntax failed due to:", e)
                logger.error("Legacy syntax failed due to:", f)
                throw f
            }
          case Failure(e) => throw e
        }
    }

  }

  /** Extract tables to be accessed by a query. */
  def extractTables(sqlQuery: String): Set[TableReference] = {
    val isLegacy = isLegacySql(sqlQuery, flattenResults = false)
    val tryJob = runQuery(sqlQuery, null, flattenResults = false, isLegacy, dryRun = true)
    Option(tryJob.get.getStatistics.getQuery.getReferencedTables) match {
      case Some(l) => l.asScala.toSet
      case None => Set.empty
    }
  }

  /** Extract locations of tables to be access by a query. */
  def extractLocation(sqlQuery: String): Option[String] = {
    val locations = extractTables(sqlQuery)
      .map(t => (t.getProjectId, t.getDatasetId))
      .map { case (pId, dId) =>
        val l = bigquery.datasets().get(pId, dId).execute().getLocation
        if (l != null) l else DEFAULT_LOCATION
      }
    require(locations.size <= 1, "Tables in the query must be in the same location")
    locations.headOption
  }

  // =======================================================================
  // Schema and query caching
  // =======================================================================

  private def withCacheKey(key: String)(method: => TableSchema): TableSchema =
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

  private def setCacheDestinationTable(key: String, table: TableReference): Unit =
    Files.write(beam.BigQueryHelpers.toTableSpec(table), tableCacheFile(key), Charsets.UTF_8)

  private def getCacheDestinationTable(key: String): Option[TableReference] = Try {
    beam.BigQueryHelpers.parseTableSpec(scala.io.Source.fromFile(tableCacheFile(key)).mkString)
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

  // =======================================================================
  // Export
  // =======================================================================

  /** Export a table as Csv */
  def exportTableAsCsv(sourceTable: String,
                       destinationUris: List[String],
                       gzipCompression: Boolean = false,
                       fieldDelimiter: Option[String] = None,
                       printHeader: Option[Boolean] = None): Unit = {

    exportTable(sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "CSV",
      gzipCompression = gzipCompression,
      fieldDelimiter = fieldDelimiter,
      printHeader = printHeader)
  }

  /** Export a table as Json */
  def exportTableAsJson(sourceTable: String,
                        destinationUris: List[String],
                        gzipCompression: Boolean = false): Unit = {

    exportTable(sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "NEWLINE_DELIMITED_JSON",
      gzipCompression = gzipCompression)
  }

  /** Export a table as Avro */
  def exportTableAsAvro(sourceTable: String,
                        destinationUris: List[String],
                        gzipCompression: Boolean = false): Unit = {

    exportTable(sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "AVRO",
      gzipCompression = gzipCompression)
  }

  private def exportTable(sourceTable: String,
                          destinationUris: List[String],
                          format: String,
                          gzipCompression: Boolean = false,
                          fieldDelimiter: Option[String] = None,
                          printHeader: Option[Boolean] = None): Unit = {

    val tableRef = beam.BigQueryHelpers.parseTableSpec(sourceTable)

    val jobConfigExtract = new JobConfigurationExtract()
      .setSourceTable(tableRef)
      .setDestinationUris(destinationUris.asJava)
      .setDestinationFormat(format)

    if (gzipCompression) jobConfigExtract.setCompression("GZIP")
    fieldDelimiter.foreach(jobConfigExtract.setFieldDelimiter)
    printHeader.foreach(jobConfigExtract.setPrintHeader(_))

    val jobConfig = new JobConfiguration()
      .setExtract(jobConfigExtract)

    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    val jobReference = new JobReference().setProjectId(projectId).setJobId(fullJobId)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)

    logger.info(s"Extracting table $sourceTable to ${destinationUris.mkString(", ")}")

    bigquery.jobs().insert(projectId, job).execute()

    val extractJob = ExtractJob(destinationUris, Some(jobReference), tableRef)

    waitForJobs(extractJob)
  }

  // =======================================================================
  // Load
  // =======================================================================

  // scalastyle:off parameter.number
  // scalastyle:off method.length
  def loadTableFromCsv(sources: List[String],
                       destinationTable: String,
                       createDisposition: CreateDisposition = CREATE_IF_NEEDED,
                       writeDisposition: WriteDisposition = WRITE_APPEND,
                       schema: Option[TableSchema] = None,
                       autodetect: Boolean = false,
                       allowJaggedRows: Boolean = false,
                       allowQuotedNewLines: Boolean = false,
                       quote: Option[String] = None,
                       maxBadRecords: Int = 0,
                       skipLeadingRows: Int = 0,
                       fieldDelimiter: Option[String] = None,
                       ignoreUnknownValues: Boolean = false,
                       encoding: Option[String] = None): TableReference = {

    loadTable(sources = sources, sourceFormat = "CSV", destinationTable = destinationTable,
      createDisposition = createDisposition, writeDisposition = writeDisposition,
      schema = schema, autodetect = Some(autodetect), allowJaggedRows = Some(allowJaggedRows),
      allowQuotedNewLines = Some(allowQuotedNewLines), quote = quote,
      maxBadRecords = maxBadRecords, skipLeadingRows = Some(skipLeadingRows),
      fieldDelimiter = fieldDelimiter,
      ignoreUnknownValues = Some(ignoreUnknownValues), encoding = encoding)
  }

  def loadTableFromJson(sources: List[String],
                        destinationTable: String,
                        createDisposition: CreateDisposition = CREATE_IF_NEEDED,
                        writeDisposition: WriteDisposition = WRITE_APPEND,
                        schema: Option[TableSchema] = None,
                        autodetect: Boolean = false,
                        maxBadRecords: Int = 0,
                        ignoreUnknownValues: Boolean = false,
                        encoding: Option[String] = None): TableReference = {

    loadTable(sources = sources, sourceFormat = "NEWLINE_DELIMITED_JSON",
      destinationTable = destinationTable,
      createDisposition = createDisposition, writeDisposition = writeDisposition,
      schema = schema, autodetect = Some(autodetect),
      maxBadRecords = maxBadRecords,
      ignoreUnknownValues = Some(ignoreUnknownValues), encoding = encoding)
  }

  def loadTableFromAvro(sources: List[String],
                        destinationTable: String,
                        createDisposition: CreateDisposition = CREATE_IF_NEEDED,
                        writeDisposition: WriteDisposition = WRITE_APPEND,
                        schema: Option[TableSchema] = None,
                        maxBadRecords: Int = 0,
                        encoding: Option[String] = None): TableReference = {

    loadTable(sources = sources, sourceFormat = "AVRO",
      destinationTable = destinationTable,
      createDisposition = createDisposition, writeDisposition = writeDisposition,
      schema = schema, maxBadRecords = maxBadRecords, encoding = encoding)
  }

  private def loadTable(sources: List[String],
                        sourceFormat: String,
                        destinationTable: String,
                        createDisposition: CreateDisposition = CREATE_IF_NEEDED,
                        writeDisposition: WriteDisposition = WRITE_APPEND,
                        schema: Option[TableSchema] = None,
                        autodetect: Option[Boolean] = None,
                        allowJaggedRows: Option[Boolean] = None,
                        allowQuotedNewLines: Option[Boolean] = None,
                        quote: Option[String] = None,
                        maxBadRecords: Int = 0,
                        skipLeadingRows: Option[Int] = None,
                        fieldDelimiter: Option[String] = None,
                        ignoreUnknownValues: Option[Boolean] = None,
                        encoding: Option[String] = None): TableReference = {

    val tableRef = beam.BigQueryHelpers.parseTableSpec(destinationTable)

    val jobConfigLoad = new JobConfigurationLoad()
      .setSourceUris(sources.asJava)
      .setSourceFormat(sourceFormat)
      .setDestinationTable(tableRef)
      .setCreateDisposition(createDisposition.toString)
      .setWriteDisposition(writeDisposition.toString)
      .setMaxBadRecords(maxBadRecords)
      .setSchema(schema.orNull)
      .setQuote(quote.orNull)
      .setFieldDelimiter(fieldDelimiter.orNull)
      .setEncoding(encoding.orNull)

    autodetect.foreach(jobConfigLoad.setAutodetect(_))
    allowJaggedRows.foreach(jobConfigLoad.setAllowJaggedRows(_))
    allowQuotedNewLines.foreach(jobConfigLoad.setAllowQuotedNewlines(_))
    skipLeadingRows.foreach(jobConfigLoad.setSkipLeadingRows(_))
    ignoreUnknownValues.foreach(jobConfigLoad.setIgnoreUnknownValues(_))

    val jobConfig = new JobConfiguration()
      .setLoad(jobConfigLoad)

    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    val jobReference = new JobReference().setProjectId(projectId).setJobId(fullJobId)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)

    logger.info(s"Loading data into $destinationTable from ${sources.mkString(", ")}")

    bigquery.jobs().insert(projectId, job).execute()

    val loadJob = LoadJob(sources, Some(jobReference), tableRef)

    waitForJobs(loadJob)

    tableRef
  }

  // scalastyle:on parameter.number
  // scalastyle:on method.length

  // =======================================================================
  // Job handling
  // =======================================================================

  /** Wait for all jobs to finish. */
  private[scio] def waitForJobs(jobs: BigQueryJob*): Unit = {
    val numTotal = jobs.size
    var pendingJobs = jobs.flatMap {
      job =>
        job.jobReference match {
          case Some(reference) => Some((job, reference))
          case None => None
        }
    }

    while (pendingJobs.nonEmpty) {
      val remainingJobs = pendingJobs.filter {
        case (bqJob, jobReference) =>
          val jobId = jobReference.getJobId
          try {
            val poll = bigquery.jobs().get(projectId, jobId).execute()
            val error = poll.getStatus.getErrorResult
            if (error != null) {
              throw new RuntimeException(
                s"${bqJob.jobType} Job failed: id: $jobId, error: $error")
            }
            if (poll.getStatus.getState == "DONE") {
              logJobStatistics(bqJob, poll)
              false
            } else {
              true
            }
          } catch {
            case e: IOException =>
              logger.warn(s"BigQuery request failed: id: $jobId, error: $e")
              true
          }
      }

      pendingJobs = remainingJobs
      val numDone = numTotal - pendingJobs.size
      logger.info(s"Job: $numDone out of $numTotal completed")
      if (pendingJobs.nonEmpty) {
        Thread.sleep(10000)
      }
    }
  }

  private def logJobStatistics(bqJob: BigQueryJob, job: Job): Unit = {

    val jobId = job.getJobReference.getJobId
    val stats = job.getStatistics
    logger.info(s"${bqJob.jobType} completed: jobId: $jobId")

    bqJob match {
      case _: ExtractJob =>
        val destinationFileCount = stats.getExtract.getDestinationUriFileCounts
          .asScala.reduce(_ + _)

        logger.info(s"Total destination file count: $destinationFileCount")

      case _: LoadJob =>
        val inputFileBytes = FileUtils.byteCountToDisplaySize(stats.getLoad.getInputFileBytes)
        val outputBytes = FileUtils.byteCountToDisplaySize(stats.getLoad.getOutputBytes)
        val outputRows = stats.getLoad.getOutputRows
        logger.info(s"Input file bytes: $inputFileBytes, output bytes: $outputBytes, " +
          s"output rows: $outputRows")

      case queryJob: QueryJob =>
        logger.info(s"Query: `${queryJob.query}`")
        val bytes = FileUtils.byteCountToDisplaySize(stats.getQuery.getTotalBytesProcessed)
        val cacheHit = stats.getQuery.getCacheHit
        logger.info(s"Total bytes processed: $bytes, cache hit: $cacheHit")
    }

    val elapsed = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getCreationTime))
    val pending = PERIOD_FORMATTER.print(new Period(stats.getStartTime - stats.getCreationTime))
    val execution = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getStartTime))
    logger.info(s"Elapsed: $elapsed, pending: $pending, execution: $execution")
  }
}
// scalastyle:on number.of.methods
// scalastyle:on file.size.limit

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

  private def isCacheEnabled: Boolean = Option(sys.props(CACHE_ENABLED_KEY))
    .flatMap(x => Try(x.toBoolean).toOption).getOrElse(CACHE_ENABLED_DEFAULT)

  private def cacheDirectory: String = getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT)

  private def connectTimeoutMs: Option[Int] = Option(sys.props(CONNECT_TIMEOUT_MS_KEY)).map(_.toInt)

  private def readTimeoutMs: Option[Int] = Option(sys.props(READ_TIMEOUT_MS_KEY)).map(_.toInt)

  private def priority: Option[String] = Option(sys.props(PRIORITY_KEY))

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
