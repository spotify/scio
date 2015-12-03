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
import com.google.api.services.bigquery.{BigqueryScopes, Bigquery}
import com.google.api.services.bigquery.model._
import com.google.cloud.dataflow.sdk.util.BigQueryTableRowIterator
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

}

/** A simple BigQuery client. */
class BigQueryClient private (private val projectId: String, credential: Credential) {

  private val bigquery: Bigquery = new Bigquery(new NetHttpTransport, new JacksonFactory, credential)

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
  def getQueryRows(sqlQuery: String): Iterator[TableRow] = getTableRows(queryIntoTable(sqlQuery))

  /** Get rows from a table. */
  def getTableRows(tableSpec: String): Iterator[TableRow] = getTableRows(Util.parseTableSpec(tableSpec))

  /** Get rows from a table. */
  def getTableRows(table: TableReference): Iterator[TableRow] =
    BigQueryTableRowIterator.fromTable(table, bigquery).asScala

  /** Get schema from a table. */
  def getTableSchema(tableSpec: String): TableSchema = getTableSchema(Util.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def getTableSchema(table: TableReference): TableSchema = withCacheKey(Util.toTableSpec(table)) {
    bigquery.tables().get(table.getProjectId, table.getDatasetId, table.getTableId).execute().getSchema
  }

  /** Execute a query and save results into a temporary table. */
  def queryIntoTable(sqlQuery: String): TableReference = {
    prepareStagingDataset()

    val destinationTable = temporaryTable(TABLE_PREFIX)

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
    val jobId = bigquery.jobs().insert(projectId, job).execute().getJobReference.getJobId

    var pollJob: Job = null
    var state: String = null
    logger.info(s"Executing BigQuery for table ${Util.toTableSpec(destinationTable)}")
    do {
      pollJob = bigquery.jobs().get(projectId, jobId).execute()
      val error = pollJob.getStatus.getErrorResult
      if (error != null) {
        throw new RuntimeException(s"BigQuery failed: $error")
      }
      state = pollJob.getStatus.getState
      logger.info(s"Job $jobId: $state")
      Thread.sleep(10000)
    } while (state != "DONE")

    logJobStatistics(pollJob)

    destinationTable
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

  // =======================================================================
  // Schema caching
  // =======================================================================

  private def withCacheKey(key: String)(method: => TableSchema): TableSchema = getCacheSchema(key) match {
    case Some(schema) => schema
    case None =>
      val schema = method
      setCacheSchema(key, schema)
      schema
  }

  private def setCacheSchema(key: String, schema: TableSchema): Unit =
    Files.write(schema.toPrettyString, cacheFile(key), Charsets.UTF_8)

  private def getCacheSchema(key: String): Option[TableSchema] = Try {
    Util.parseSchema(scala.io.Source.fromFile(cacheFile(key)).mkString)
  }.toOption

  private def cacheFile(key: String): File = {
    val cacheDir = BigQueryClient.cacheDirectory
    val outputFile = new File(cacheDir)
    if (!outputFile.exists()) {
      outputFile.mkdirs()
    }
    val filename = Hashing.sha1().hashString(key, Charsets.UTF_8).toString.substring(0, 32) + ".json"
    new File(s"$cacheDir/$filename")
  }

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
   * Create a new BigQueryClient instance with project and JSON secret from system properties.
   *
   * Project and path to JSON secret must be set in `bigquery.project` and `bigquery.secret`
   * system properties. For example, by adding the following to your job code:
   *
   * {{{
   * sys.props("bigquery.project") = "my-project"
   * sys.props("bigquery.secret") = "/path/to/secret.json"
   * }}}
   *
   * Or you can pass them as SBT command line arguments:
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
    val secret = sys.props(SECRET_KEY)
    if (secret == null) {
      throw new RuntimeException(
        s"Property $SECRET_KEY not set. Use -D$SECRET_KEY=/path/to/secret.json")
    }
    val scopes = List(BigqueryScopes.BIGQUERY).asJava
    val credential = GoogleCredential.fromStream(new FileInputStream(new File(secret))).createScoped(scopes)

    BigQueryClient(project, credential)
  }

  private def stagingDataset: String = getPropOrElse(STAGING_DATASET_KEY, STAGING_DATASET_DEFAULT)

  private def cacheDirectory: String = getPropOrElse(CACHE_DIRECTORY_KEY, CACHE_DIRECTORY_DEFAULT)

  private def getPropOrElse(key: String, default: String): String = {
    val value = sys.props(key)
    if (value == null) default else value
  }

}
