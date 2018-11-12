/*
 * Copyright 2018 Spotify AB.
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

package com.spotify.scio.bigquery.client

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.model._
import com.spotify.scio.bigquery.client.BigQuery.Client
import com.spotify.scio.bigquery.{BigQueryUtil, TableRow}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[client] object QueryOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private def isInteractive =
    BigQueryConfig.priority
      .map(_ == "INTERACTIVE")
      .getOrElse {
        Thread
          .currentThread()
          .getStackTrace
          .exists { e =>
            e.getClassName.startsWith("scala.tools.nsc.interpreter.") ||
            e.getClassName.startsWith("org.scalatest.tools.")
          }
      }

  private val Priority = if (isInteractive) "INTERACTIVE" else "BATCH"
}

private[client] final class QueryOps(client: Client, tableService: TableOps, jobService: JobOps) {
  import QueryOps._

  /** Get schema for a query without executing it. */
  def schema(sqlQuery: String): TableSchema = Cache.withCacheKey(sqlQuery) {
    if (isLegacySql(sqlQuery)) {
      // Dry-run not supported for legacy query, using view as a work around
      Logger.info("Getting legacy query schema with view")
      val location = extractLocation(sqlQuery).getOrElse(BigQueryConfig.location)
      tableService.prepareStagingDataset(location)
      val temp = tableService.createTemporary(location)

      // Create temporary table view and get schema
      Logger.info(s"Creating temporary view ${bq.BigQueryHelpers.toTableSpec(temp)}")
      val view = new ViewDefinition().setQuery(sqlQuery)
      val viewTable = new Table().setView(view).setTableReference(temp)
      val schema = client.underlying
        .tables()
        .insert(temp.getProjectId, temp.getDatasetId, viewTable)
        .execute()
        .getSchema

      // Delete temporary table
      Logger.info(s"Deleting temporary view ${bq.BigQueryHelpers.toTableSpec(temp)}")
      client.underlying
        .tables()
        .delete(temp.getProjectId, temp.getDatasetId, temp.getTableId)
        .execute()

      schema
    } else {
      // Get query schema via dry-run
      Logger.info("Getting SQL query schema with dry-run")
      val jobConfig = QueryJobConfig(sqlQuery, dryRun = true, useLegacySql = false)
      run(jobConfig).get.getStatistics.getQuery.getSchema
    }
  }

  /** Get rows from a query. */
  def rows(
    sqlQuery: String,
    flattenResults: Boolean = false,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_EMPTY,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED
  ): Iterator[TableRow] = {
    val queryJob = newQueryJob(
      QueryJobConfig(sqlQuery,
                     useLegacySql = isLegacySql(sqlQuery),
                     flattenResults = flattenResults,
                     writeDisposition = writeDisposition,
                     createDisposition = createDisposition))
    jobService.waitForJobs(queryJob)
    tableService.rows(queryJob.table)
  }

  // =======================================================================
  // Query handling
  // =======================================================================
  private[scio] def newQueryJob(querySql: String, flattenResults: Boolean = false): QueryJob = {
    newQueryJob(
      QueryJobConfig(querySql,
                     useLegacySql = isLegacySql(querySql),
                     flattenResults = flattenResults))
  }

  private[scio] def newQueryJob(query: QueryJobConfig): QueryJob = {
    if (BigQueryConfig.isCacheEnabled) {
      newCachedQueryJob(query)
    } else {
      Logger.info(s"BigQuery caching is disabled")
      val tempTable = tableService.createTemporary(
        extractLocation(query.sql)
          .getOrElse(BigQueryConfig.location))
      delayedQueryJob(query.withDestination(tempTable))
    }
  }

  private[scio] def newCachedQueryJob(query: QueryJobConfig): QueryJob = {
    try {
      val sourceTimes = extractTables(query.sql)
        .map(t => BigInt(tableService.table(t).getLastModifiedTime))
      val temp = Cache.getCacheDestinationTable(query.sql).get
      val time = BigInt(tableService.table(temp).getLastModifiedTime)
      if (sourceTimes.forall(_ < time)) {
        Logger.info(s"Cache hit for query: `${query.sql}`")
        Logger.info(s"Existing destination table: ${bq.BigQueryHelpers.toTableSpec(temp)}")
        QueryJob(query.sql, jobReference = None, table = temp)
      } else {
        Logger.info(s"Cache invalid for query: `${query.sql}`")
        val newTemp = tableService.createTemporary(
          extractLocation(query.sql)
            .getOrElse(BigQueryConfig.location))
        Logger.info(s"New destination table: ${bq.BigQueryHelpers.toTableSpec(newTemp)}")
        Cache.setCacheDestinationTable(query.sql, newTemp)
        delayedQueryJob(query.withDestination(newTemp))
      }
    } catch {
      case NonFatal(e: GoogleJsonResponseException) if isInvalidQuery(e) => throw e
      case NonFatal(_) =>
        val temp = tableService.createTemporary(
          extractLocation(query.sql)
            .getOrElse(BigQueryConfig.location))
        Logger.info(s"Cache miss for query: `${query.sql}`")
        Logger.info(s"New destination table: ${bq.BigQueryHelpers.toTableSpec(temp)}")
        Cache.setCacheDestinationTable(query.sql, temp)
        delayedQueryJob(query.withDestination(temp))
    }
  }

  private def delayedQueryJob(query: QueryJobConfig): QueryJob = {
    val jobReference = {
      val location = extractLocation(query.sql).getOrElse(BigQueryConfig.location)
      tableService.prepareStagingDataset(location)
      val isLegacy = isLegacySql(query.sql)

      val tryRun = run(query)
      Some(tryRun.get.getJobReference)
    }

    QueryJob(query.sql, jobReference, query.destinationTable)
  }

  private val dryRunCache: MMap[(String, Boolean, Boolean), Try[Job]] = MMap.empty

  /* Creates and submits a query job */
  def run(
    sqlQuery: String,
    destinationTable: String = null,
    flattenResults: Boolean = false,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_EMPTY,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED): TableReference = {
    val query = QueryJobConfig(sqlQuery,
                               useLegacySql = isLegacySql(sqlQuery),
                               flattenResults = flattenResults,
                               writeDisposition = writeDisposition,
                               createDisposition = createDisposition)
    if (destinationTable != null) {
      val tableRef = bq.BigQueryHelpers.parseTableSpec(destinationTable)
      val queryJob = delayedQueryJob(query.withDestination(tableRef))
      jobService.waitForJobs(queryJob)
      tableRef
    } else {
      val queryJob = newQueryJob(query)
      jobService.waitForJobs(queryJob)
      queryJob.table
    }
  }

  private[scio] case class QueryJobConfig(
    sql: String,
    useLegacySql: Boolean,
    dryRun: Boolean = false,
    destinationTable: TableReference = null,
    flattenResults: Boolean = false,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_EMPTY,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED) {

    def withDestination(table: TableReference): QueryJobConfig = copy(destinationTable = table)
  }

  /* Creates and submits a query job */
  private def run(config: QueryJobConfig): Try[Job] = {
    def run = Try {
      val queryConfig = new JobConfigurationQuery()
        .setQuery(config.sql)
        .setUseLegacySql(config.useLegacySql)
        .setFlattenResults(config.flattenResults)
        .setPriority(Priority)
        .setCreateDisposition(config.createDisposition.name)
        .setWriteDisposition(config.writeDisposition.name)
      if (!config.dryRun) {
        queryConfig.setAllowLargeResults(true).setDestinationTable(config.destinationTable)
      }
      val jobConfig = new JobConfiguration().setQuery(queryConfig).setDryRun(config.dryRun)
      val fullJobId = BigQueryUtil.generateJobId(client.project)
      val jobReference = new JobReference().setProjectId(client.project).setJobId(fullJobId)
      val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
      client.underlying.jobs().insert(client.project, job).execute()
    }
    if (config.useLegacySql) {
      Logger.info(s"Executing legacy query: `${config.sql}`")
    } else {
      Logger.info(s"Executing standard SQL query: `${config.sql}`")
    }
    if (config.dryRun) {
      dryRunCache.getOrElseUpdate((config.sql, config.flattenResults, config.useLegacySql), run)
    } else {
      run
    }
  }

  private def isInvalidQuery(e: GoogleJsonResponseException): Boolean =
    e.getDetails.getErrors.get(0).getReason == "invalidQuery"

  private[scio] def isLegacySql(sqlQuery: String, flattenResults: Boolean = false): Boolean = {
    val dryRun =
      QueryJobConfig(sqlQuery, dryRun = true, useLegacySql = false, flattenResults = flattenResults)

    sqlQuery.trim.split("\n")(0).trim.toLowerCase match {
      case "#legacysql"   => true
      case "#standardsql" => false
      case _              =>
        // dry run with SQL syntax first
        run(dryRun) match {
          case Success(_)                                                   => false
          case Failure(e: GoogleJsonResponseException) if isInvalidQuery(e) =>
            // dry run with legacy syntax next
            run(dryRun.copy(useLegacySql = true)) match {
              case Success(_) =>
                Logger.warn(
                  "Legacy syntax is deprecated, use SQL syntax instead. " +
                    "See https://cloud.google.com/bigquery/docs/reference/standard-sql/")
                Logger.warn(s"Legacy query: `$sqlQuery`")
                true
              case Failure(f) =>
                Logger.error(
                  s"Tried both standard and legacy syntax, query `$sqlQuery` failed for both!")
                Logger.error("Standard syntax failed due to:", e)
                Logger.error("Legacy syntax failed due to:", f)
                throw f
            }
          case Failure(e) => throw e
        }
    }

  }

  /** Extract tables to be accessed by a query. */
  def extractTables(sqlQuery: String): Set[TableReference] = {
    val tryJob = run(QueryJobConfig(sqlQuery, dryRun = true, useLegacySql = isLegacySql(sqlQuery)))
    Option(tryJob.get.getStatistics.getQuery.getReferencedTables) match {
      case Some(l) => l.asScala.toSet
      case None    => Set.empty
    }
  }

  /** Extract locations of tables to be accessed by a query. */
  def extractLocation(sqlQuery: String): Option[String] = {
    val locations = extractTables(sqlQuery)
      .map(t => (t.getProjectId, t.getDatasetId))
      .map {
        case (pId, dId) =>
          val l = client.underlying.datasets().get(pId, dId).execute().getLocation
          if (l != null) l else BigQueryConfig.location
      }
    require(locations.size <= 1, "Tables in the query must be in the same location")
    locations.headOption
  }

}
