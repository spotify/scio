package com.spotify.cloud.bigquery

import java.io.{StringReader, FileInputStream, File}
import java.util.regex.Pattern

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{BigqueryScopes, Bigquery}
import com.google.api.services.bigquery.model._
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.google.common.io.Files
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

  /** Get schema for a query without executing it. */
  def getQuerySchema(sqlQuery: String): TableSchema = withCacheKey(sqlQuery) {
    prepareStagingDataset()

    // Create temporary table view and get schema
    val table = temporaryTable("query_schema")
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
  def getTableRows(table: TableReference): Iterator[TableRow] = new BigQueryTableRowIterator(bigquery, table).asScala

  /** Get schema from a table. */
  def getTableSchema(tableSpec: String): TableSchema = getTableSchema(Util.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def getTableSchema(table: TableReference): TableSchema = withCacheKey(Util.toTableSpec(table)) {
    bigquery.tables().get(table.getProjectId, table.getDatasetId, table.getTableId).execute().getSchema
  }

  /** Execute a query and save results into a temporary table. */
  def queryIntoTable(sqlQuery: String, tableSpec: String = null): TableReference = {
    prepareStagingDataset()

    val destinationTable = if (tableSpec == null) temporaryTable("query_into_table") else Util.parseTableSpec(tableSpec)

    val queryConfig: JobConfigurationQuery = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setAllowLargeResults(true)
      .setFlattenResults(false)
      .setPriority("BATCH")
      .setCreateDisposition("CREATE_IF_NEEDED")
      .setWriteDisposition("WRITE_EMPTY")
      .setDestinationTable(destinationTable)

    val jobConfig: JobConfiguration = new JobConfiguration().setQuery(queryConfig)
    val job = new Job().setConfiguration(jobConfig)

    val insert = bigquery.jobs().insert(projectId, job)
    val jobId = insert.execute().getJobReference

    var pollJob: Job = null
    var state: String = null
    logger.info(s"Executing BigQuery for table ${Util.toTableSpec(destinationTable)}")
    do {
      pollJob = bigquery.jobs().get(projectId, jobId.getJobId).execute()
      val error = pollJob.getStatus.getErrorResult
      if (error != null) {
        throw new RuntimeException(s"BigQuery failed: $error")
      }
      state = pollJob.getStatus.getState
      logger.info(s"Job ${jobId.getJobId}: $state")
      Thread.sleep(10000)
    } while (state != "DONE")

    destinationTable
  }

  private def prepareStagingDataset(): Unit = {
    // Create staging dataset if it does not already exist
    val datasetId = BigQueryClient.STAGING_DATASET
    try {
      bigquery.datasets().get(projectId, datasetId).execute()
      logger.info(s"Staging dataset $projectId:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if e.getStatusCode == 404 =>
        logger.info(s"Creating staging dataset $projectId:$datasetId")
        val ds = new DatasetReference().setProjectId(projectId).setDatasetId(datasetId)
        bigquery
          .datasets()
          .insert(projectId, new Dataset().setDatasetReference(ds))
          .execute()
      case e: Throwable => throw e
    }
  }

  private def temporaryTable(prefix: String): TableReference = {
    val tableId = prefix + "_" + System.currentTimeMillis() + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(projectId)
      .setDatasetId(BigQueryClient.STAGING_DATASET)
      .setTableId(tableId)
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
    val cacheDir = sys.props("bigquery.cache.directory")
    val outputDir = if (cacheDir != null) cacheDir else sys.props("user.dir") + "/.bigquery"
    val outputFile = new File(outputDir)
    if (!outputFile.exists()) {
      outputFile.mkdirs()
    }
    val filename = Hashing.sha1().hashString(key, Charsets.UTF_8).toString.substring(0, 32) + ".json"
    new File(s"$outputDir/$filename")
  }

}

/** Companion object for [[BigQueryClient]]. */
object BigQueryClient {

  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava

  /** BigQuery dataset for staging results like temporary tables and views. */
  val STAGING_DATASET = "bigquery_staging"

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
   *
   * The client also caches schemas from tables or queries. The cached files are stored in
   * `[PROJECT_ROOT]/.bigquery` by default but can be overridden with `bigquery.cache.directory`
   * system property.
   */
  def apply(): BigQueryClient = {
    val project = sys.props("bigquery.project")
    if (project == null) {
      throw new RuntimeException("Property bigquery.project not set. Use -Dbigquery.project=<BILLING_PROJECT>")
    }
    val secret = sys.props("bigquery.secret")
    if (secret == null) {
      throw new RuntimeException("Property bigquery.secret not set. Use -Dbigquery.secret=/path/to/secret.json")
    }
    val credential = GoogleCredential.fromStream(new FileInputStream(new File(secret))).createScoped(SCOPES)

    BigQueryClient(project, credential)
  }

}
