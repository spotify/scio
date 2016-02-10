package com.spotify.scio.bigquery

import java.io.{StringReader, FileInputStream, File}
import java.util.UUID
import java.util.regex.Pattern

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery.Builder
import com.google.api.services.bigquery.{BigqueryScopes, Bigquery}
import com.google.api.services.bigquery.model._
import com.google.cloud.dataflow.sdk.options.{PipelineOptionsFactory, GcpOptions}
import com.google.cloud.dataflow.sdk.util.{Credentials, BigQueryTableRowIterator}
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
import org.apache.commons.io.FileUtils
import org.joda.time.{Period, Instant}
import org.joda.time.format.{PeriodFormatterBuilder, DateTimeFormat}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.util.{Try, Random}

/** Utility for BigQuery data types. */
object Util {

  // Ported from com.google.cloud.dataflow.sdk.io.BigQueryIO

  private val PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]"
  private val DATASET_REGEXP = "[-\\w.]{1,1024}"
  private val TABLE_REGEXP = "[-\\w$@]{1,1024}"
  private val DATASET_TABLE_REGEXP =
    s"((?<PROJECT>$PROJECT_ID_REGEXP):)?(?<DATASET>$DATASET_REGEXP)\\.(?<TABLE>$TABLE_REGEXP)"
  private val TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP)
  private val QUERY_TABLE_SPEC = Pattern.compile(s"(?<=\\[)$DATASET_REGEXP(?=\\])")

  /** Parse a table specification string. */
  def parseTableSpec(tableSpec: String): TableReference = {
    val m = TABLE_SPEC.matcher(tableSpec)
    require(m.matches(), s"Table reference is not in [project_id]:[dataset_id].[table_id] format: $tableSpec")
    new TableReference()
      .setProjectId(m.group("PROJECT"))
      .setDatasetId(m.group("DATASET"))
      .setTableId(m.group("TABLE"))
  }

  /** Convert a table reference to string. */
  def toTableSpec(table: TableReference): String =
    (if (table.getProjectId != null) table.getProjectId + ":" else "") + table.getDatasetId + "."  + table.getTableId

  /** Parse a schema string. */
  def parseSchema(schemaString: String): TableSchema =
    new JsonObjectParser(new JacksonFactory).parseAndClose(new StringReader(schemaString), classOf[TableSchema])

  /** Extract tables from a SQL query. */
  def extractTables(sqlQuery: String): Set[TableReference] = {
    val matcher = Util.QUERY_TABLE_SPEC.matcher(sqlQuery)
    val b = Set.newBuilder[TableReference]
    while (matcher.find()) {
      b += parseTableSpec(matcher.group())
    }
    b.result()
  }

}

/** A simple BigQuery client. */
class BigQueryClient private (private val projectId: String, credential: Credential) {

  private val bigquery: Bigquery =
    new Builder(new NetHttpTransport, new JacksonFactory, credential)
      .setApplicationName("scio")
      .build()

  private val logger: Logger = LoggerFactory.getLogger(classOf[BigQueryClient])

  private val TABLE_PREFIX = "dataflow_query"
  private val JOB_ID_PREFIX = "dataflow_query"
  private val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  private val PERIOD_FORMATTER = new PeriodFormatterBuilder()
    .appendHours().appendSuffix("h")
    .appendMinutes().appendSuffix("m")
    .appendSecondsWithOptionalMillis().appendSuffix("s")
    .toFormatter

  private val PRIORITY =
    if (Thread.currentThread().getStackTrace.exists(_.getClassName.startsWith("scala.tools.nsc.interpreter."))) {
      "INTERACTIVE"
    } else {
      "BATCH"
    }

  /** Get schema for a query without executing it. */
  def getQuerySchema(sqlQuery: String): TableSchema = withCacheKey(sqlQuery) {
    prepareStagingDataset()

    // Create temporary table view and get schema
    val table = temporaryTable(TABLE_PREFIX)
    logger.info(s"Creating temporary view ${Util.toTableSpec(table)}")
    val view = new ViewDefinition().setQuery(sqlQuery)
    val viewTable = new Table().setView(view).setTableReference(table)
    val schema = bigquery.tables().insert(table.getProjectId, table.getDatasetId, viewTable).execute().getSchema

    // Delete temporary table
    logger.info(s"Deleting temporary view ${Util.toTableSpec(table)}")
    bigquery.tables().delete(table.getProjectId, table.getDatasetId, table.getTableId).execute()

    schema
  }

  /** Get rows from a query. */
  def getQueryRows(sqlQuery: String): Iterator[TableRow] = {
    val (tableRef, jobRef) = queryIntoTable(sqlQuery)
    jobRef.foreach(j => waitForJobs(j))
    getTableRows(tableRef)
  }

  /** Get rows from a table. */
  def getTableRows(tableSpec: String): Iterator[TableRow] = getTableRows(Util.parseTableSpec(tableSpec))

  /** Get rows from a table. */
  def getTableRows(table: TableReference): Iterator[TableRow] =
    BigQueryTableRowIterator.fromTable(table, bigquery).asScala

  /** Get schema from a table. */
  def getTableSchema(tableSpec: String): TableSchema = getTableSchema(Util.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def getTableSchema(table: TableReference): TableSchema = withCacheKey(Util.toTableSpec(table)) {
    getTable(table).getSchema
  }

  /** Execute a query and save results into a temporary table. */
  def queryIntoTable(sqlQuery: String): (TableReference, Option[JobReference]) = {
    prepareStagingDataset()

    logger.info(s"Executing BigQuery for query: $sqlQuery")

    try {
      val sourceTimes = Util.extractTables(sqlQuery).map(t => BigInt(getTable(t).getLastModifiedTime))
      val table = getCacheDestinationTable(sqlQuery).get
      val time = BigInt(getTable(table).getLastModifiedTime)
      if (sourceTimes.forall(_ < time)) {
        logger.info(s"Cache hit, existing destination table: ${Util.toTableSpec(table)}")
        (table, None)
      } else {
        val temp = temporaryTable(TABLE_PREFIX)
        logger.info(s"Cache invalid, new destination table: ${Util.toTableSpec(temp)}")
        setCacheDestinationTable(sqlQuery, temp)
        (temp, Some(makeQuery(sqlQuery, temp)))
      }
    } catch {
      case _: Throwable =>
        val temp = temporaryTable(TABLE_PREFIX)
        logger.info(s"Cache miss, new destination table: ${Util.toTableSpec(temp)}")
        setCacheDestinationTable(sqlQuery, temp)
        (temp, Some(makeQuery(sqlQuery, temp)))
    }
  }

  /** Wait for all jobs to finish. */
  def waitForJobs(jobReferences: JobReference*): Unit = {
    val ids = jobReferences.map(_.getJobId).toBuffer
    var allDone: Boolean = false
    while (!allDone && ids.nonEmpty) {
      val pollJobs = ids.map(bigquery.jobs().get(projectId, _).execute())
      pollJobs.foreach { j =>
        val error = j.getStatus.getErrorResult
        if (error != null) {
          throw new RuntimeException(s"BigQuery failed: $error")
        }
      }
      val done = pollJobs.count(_.getStatus.getState == "DONE")
      logger.info(s"BigQuery jobs: $done out of ${pollJobs.size}")
      allDone = done == pollJobs.size
      if (allDone) {
        pollJobs.foreach(logJobStatistics)
      } else {
        Thread.sleep(10000)
      }
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
      case e: Throwable => throw e
    }
  }

  private def temporaryTable(prefix: String): TableReference = {
    val tableId = prefix + "_" + Instant.now().toString(TIME_FORMATTER) + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(BigQueryClient.stagingDataset)
      .setTableId(tableId)
  }

  private def createJobReference(projectId: String, jobIdPrefix: String): JobReference = {
    val fullJobId = projectId + "-" + UUID.randomUUID().toString
    new JobReference().setProjectId(projectId).setJobId(fullJobId)
  }

  private def makeQuery(sqlQuery: String, destinationTable: TableReference): JobReference = {
    val queryConfig: JobConfigurationQuery = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setAllowLargeResults(true)
      .setFlattenResults(false)
      .setPriority(PRIORITY)
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")
      .setDestinationTable(destinationTable)

    val jobConfig = new JobConfiguration().setQuery(queryConfig)
    val jobReference = createJobReference(projectId, JOB_ID_PREFIX)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
    bigquery.jobs().insert(projectId, job).execute().getJobReference
  }

  private def logJobStatistics(job: Job): Unit = {
    val jobId = job.getJobReference.getJobId
    val stats = job.getStatistics

    val elapsed = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getCreationTime))
    val pending = PERIOD_FORMATTER.print(new Period(stats.getStartTime - stats.getCreationTime))
    val execution = PERIOD_FORMATTER.print(new Period(stats.getEndTime - stats.getStartTime))
    logger.info(s"Job $jobId: elapsed: $elapsed, pending: $pending, execution: $execution")

    val bytes = FileUtils.byteCountToDisplaySize(stats.getQuery.getTotalBytesProcessed)
    val cacheHit = stats.getQuery.getCacheHit
    logger.info(s"Job $jobId: total bytes processed: $bytes, cache hit: $cacheHit")
  }

  private def getTable(table: TableReference): Table = {
    val p = if (table.getProjectId == null) this.projectId else table.getProjectId
    bigquery.tables().get(p, table.getDatasetId, table.getTableId).execute()
  }

  // =======================================================================
  // Schema and query caching
  // =======================================================================

  private def withCacheKey(key: String)(method: => TableSchema): TableSchema = getCacheSchema(key) match {
    case Some(schema) => schema
    case None =>
      val schema = method
      setCacheSchema(key, schema)
      schema
  }

  private def setCacheSchema(key: String, schema: TableSchema): Unit =
    Files.write(schema.toPrettyString, schemaCacheFile(key), Charsets.UTF_8)

  private def getCacheSchema(key: String): Option[TableSchema] = Try {
    Util.parseSchema(scala.io.Source.fromFile(schemaCacheFile(key)).mkString)
  }.toOption

  private def setCacheDestinationTable(key: String, table: TableReference): Unit =
    Files.write(Util.toTableSpec(table), tableCacheFile(key), Charsets.UTF_8)

  private def getCacheDestinationTable(key: String): Option[TableReference] = Try {
    Util.parseTableSpec(scala.io.Source.fromFile(tableCacheFile(key)).mkString)
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

/** Companion object for [[BigQueryClient]]. */
object BigQueryClient {

  /** System property key for billing project. */
  val PROJECT_KEY: String = "bigquery.project"

  /** System property key for JSON secret path. */
  val SECRET_KEY: String = "bigquery.secret"

  /** System property key for staging dataset. */
  val STAGING_DATASET_KEY: String = "bigquery.staging_dataset"

  /** Default staging dataset. */
  val STAGING_DATASET_DEFAULT: String = "bigquery_staging"

  /** System property key for staging dataset location. */
  val STAGING_DATASET_LOCATION_KEY: String = "bigquery.staging_dataset.location"

  /** Default staging dataset location. */
  val STAGING_DATASET_LOCATION_DEFAULT: String = "US"

  /** System property key for local schema cache directory. */
  val CACHE_DIRECTORY_KEY: String = "bigquery.cache.directory"

  /** Default cache directory. */
  val CACHE_DIRECTORY_DEFAULT: String = sys.props("user.dir") + "/.bigquery"

  /** Table expiration in milliseconds for staging dataset. */
  val STAGING_DATASET_TABLE_EXPIRATION_MS: Long = 86400000L

  /** Description for staging dataset. */
  val STAGING_DATASET_DESCRIPTION: String = "Staging dataset for temporary tables"

  /** Create a new BigQueryClient instance with the given project and credential. */
  def apply(project: String, credential: Credential): BigQueryClient = new BigQueryClient(project, credential)

  /**
   * Create a new BigQueryClient instance.
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
  def apply(): BigQueryClient = {
    val project = sys.props(PROJECT_KEY)
    if (project == null) {
      throw new RuntimeException(
        s"Property $PROJECT_KEY not set. Use -D$PROJECT_KEY=<BILLING_PROJECT>")
    }
    BigQueryClient(project)
  }

  /** Create a new BigQueryClient instance with the given project. */
  def apply(project: String): BigQueryClient = {
    val secret = sys.props(SECRET_KEY)
    if (secret == null) {
      val opts = PipelineOptionsFactory.fromArgs(Array.empty).as(classOf[GcpOptions])
      opts.setProject(project)
      val credential = Credentials.getCredential(opts)
      BigQueryClient(project, credential)
    } else {
      BigQueryClient(project, secret)
    }
  }

  /** Create a new BigQueryClient instance with the given project and secret file. */
  def apply(project: String, secret: String): BigQueryClient = {
    val scopes = List(BigqueryScopes.BIGQUERY).asJava
    val credential = GoogleCredential.fromStream(new FileInputStream(new File(secret))).createScoped(scopes)
    BigQueryClient(project, credential)
  }

  private def stagingDataset: String =
    getPropOrElse(STAGING_DATASET_KEY, STAGING_DATASET_DEFAULT + "_" + stagingDatasetLocation.toLowerCase)

  private def stagingDatasetLocation: String =
    getPropOrElse(STAGING_DATASET_LOCATION_KEY, STAGING_DATASET_LOCATION_DEFAULT)

  private def cacheDirectory: String = getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT)

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
