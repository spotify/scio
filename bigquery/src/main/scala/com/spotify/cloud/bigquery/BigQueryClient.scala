package com.spotify.cloud.bigquery

import java.io.{FileInputStream, File}
import java.util.regex.Pattern

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.{BigqueryScopes, Bigquery}
import com.google.api.services.bigquery.model._

import scala.collection.JavaConverters._

class BigQueryClient private (credential: Credential) {

  private val bigquery = new Bigquery(new NetHttpTransport, new JacksonFactory, credential)


  // Ported from com.google.cloud.dataflow.sdk.io.BigQueryIO

  private val PROJECT_ID_REGEXP = "[a-z][-a-z0-9:.]{4,61}[a-z0-9]"
  private val DATASET_REGEXP = "[-\\w.]{1,1024}"
  private val TABLE_REGEXP = "[-\\w$@]{1,1024}"
  private val DATASET_TABLE_REGEXP =
    String.format(
      "((?<PROJECT>%s):)?(?<DATASET>%s)\\.(?<TABLE>%s)",
      PROJECT_ID_REGEXP, DATASET_REGEXP, TABLE_REGEXP)
  private val TABLE_SPEC = Pattern.compile(DATASET_TABLE_REGEXP)

  private def parseTableSpec(tableSpec: String): TableReference = {
    val m = TABLE_SPEC.matcher(tableSpec)
    if (!m.matches()) {
      throw new IllegalArgumentException(
        "Table reference is not in [project_id]:[dataset_id].[table_id] format:" + tableSpec)
    }
    new TableReference()
      .setProjectId(m.group("PROJECT"))
      .setDatasetId(m.group("DATASET"))
      .setTableId(m.group("TABLE"))
  }

  def queryIntoTable(sqlQuery: String, tableSpec: String): TableReference =
    this.queryIntoTable(sqlQuery, parseTableSpec(tableSpec))

  def queryIntoTable(sqlQuery: String, table: TableReference): TableReference = {
    val queryConfig: JobConfigurationQuery = new JobConfigurationQuery()
      .setQuery(sqlQuery)
      .setAllowLargeResults(true)
      .setFlattenResults(false)
      .setPriority("BATCH")
      .setDestinationTable(table)

    val jobConfig: JobConfiguration = new JobConfiguration().setQuery(queryConfig)
    val job = new Job().setConfiguration(jobConfig)

    val insert = bigquery.jobs().insert(table.getProjectId, job)
    val jobId = insert.execute().getJobReference

    val startTime = System.currentTimeMillis()

    var pollJob: Job = null
    var state: String = null
    do {
      pollJob = bigquery.jobs().get(table.getProjectId, jobId.getJobId).execute()
      val error = pollJob.getStatus.getErrorResult
      if (error != null) {
        throw new RuntimeException(s"BigQuery failed: $error")
      }
      state = pollJob.getStatus.getState
      val ms = System.currentTimeMillis() - startTime
      // TODO: replace with logger
      println(s"Job status ($ms): ${jobId.getJobId}: $state")
      Thread.sleep(1000)
    } while (state != "DONE")

    table
  }

  def getSchema(tableSpec: String): TableSchema = getSchema(parseTableSpec(tableSpec))

  def getSchema(table: TableReference): TableSchema =
    bigquery.tables().get(table.getProjectId, table.getDatasetId, table.getTableId).execute().getSchema

}

object BigQueryClient {

  private val SCOPES = List(BigqueryScopes.BIGQUERY).asJava

  def apply(credential: Credential): BigQueryClient = new BigQueryClient(credential)

  // sbt -Dbigquery.secret=/path/to/bigquery.json
  def apply(): BigQueryClient = {
    val json = sys.props("bigquery.secret")
    if (json == null) {
      throw new RuntimeException("Property bigquery.secret not set")
    }
    fromJson(json)
  }

  def fromJson(file: String): BigQueryClient = {
    val json = GoogleCredential.fromStream(new FileInputStream(new File(file))).createScoped(SCOPES)
    new BigQueryClient(json)
  }

}