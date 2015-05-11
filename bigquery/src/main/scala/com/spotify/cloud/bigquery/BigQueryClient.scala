package com.spotify.cloud.bigquery

import com.google.api.client.auth.oauth2.Credential
import com.google.api.client.http.javanet.NetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import com.google.cloud.dataflow.sdk.io.BigQueryIO
import com.google.cloud.dataflow.sdk.options.{DataflowPipelineOptions, PipelineOptionsFactory}

class BigQueryClient(credential: Credential = null) {

  private def defaultCredential: Credential = PipelineOptionsFactory
    .fromArgs(Array())
    .withValidation()
    .as(classOf[DataflowPipelineOptions])
    .getGcpCredential

  private val _credential = if (credential != null) credential else defaultCredential

  private val bigquery = new Bigquery(new NetHttpTransport, new JacksonFactory, _credential)

  def queryIntoTable(sqlQuery: String, tableSpec: String): TableReference =
    this.queryIntoTable(sqlQuery, BigQueryIO.parseTableSpec(tableSpec))

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
      println(s"Job status ($ms): ${jobId.getJobId}: $state")
      Thread.sleep(1000)
    } while (state != "DONE")

    table
  }

  def getSchema(tableSpec: String): TableSchema = getSchema(BigQueryIO.parseTableSpec(tableSpec))

  def getSchema(table: TableReference): TableSchema =
    bigquery.tables().get(table.getProjectId, table.getDatasetId, table.getTableId).execute().getSchema

}