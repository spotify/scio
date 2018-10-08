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

package com.spotify.scio.bigquery

import com.google.api.client.googleapis.json.GoogleJsonResponseException
import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.{Map => MMap}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[scio] class QueryService(private val projectId: String,
                                 private val bigquery: Bigquery,
                                 private val tables: TableService) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private def isInteractive =
    BigQueryConfig.priority
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

  /** Get schema for a query without executing it. */
  def getSchema(sqlQuery: String): TableSchema = {
    if (isLegacySql(sqlQuery, flattenResults = false)) {
      // Dry-run not supported for legacy query, using view as a work around
      logger.info("Getting legacy query schema with view")
      val location = extractLocation(sqlQuery).getOrElse(tables.DEFAULT_LOCATION)
      tables.prepareStagingDataset(location)
      val temp = tables.createTemporary(location)

      // Create temporary table view and get schema
      logger.info(s"Creating temporary view ${bq.BigQueryHelpers.toTableSpec(temp)}")
      val view = new ViewDefinition().setQuery(sqlQuery)
      val viewTable = new Table().setView(view).setTableReference(temp)
      val schema = bigquery
        .tables()
        .insert(temp.getProjectId, temp.getDatasetId, viewTable)
        .execute()
        .getSchema

      // Delete temporary table
      logger.info(s"Deleting temporary view ${bq.BigQueryHelpers.toTableSpec(temp)}")
      bigquery.tables().delete(temp.getProjectId, temp.getDatasetId, temp.getTableId).execute()

      schema
    } else {
      // Get query schema via dry-run
      logger.info("Getting SQL query schema with dry-run")
      // scalastyle:off line.size.limit
      run(sqlQuery, null, flattenResults = false, useLegacySql = false, dryRun = true).get.getStatistics.getQuery.getSchema
      // scalastyle:on line.size.limit
    }
  }

  /** Get rows from a query. */
  def getRows(sqlQuery: String, flattenResults: Boolean = false): Iterator[TableRow] = {
    val queryJob = newQueryJob(sqlQuery, flattenResults)
    JobService.waitForJobs(projectId, bigquery, queryJob)
    tables.getRows(queryJob.table)
  }

  // =======================================================================
  // Query handling
  // =======================================================================

  private[scio] def newQueryJob(sqlQuery: String, flattenResults: Boolean): QueryJob = {
    if (BigQueryConfig.isCacheEnabled) {
      newCachedQueryJob(sqlQuery, flattenResults)
    } else {
      logger.info(s"BigQuery caching is disabled")
      val tempTable = tables.createTemporary(
        extractLocation(sqlQuery)
          .getOrElse(tables.DEFAULT_LOCATION))
      delayedQueryJob(sqlQuery, tempTable, flattenResults)
    }
  }

  private[scio] def newCachedQueryJob(sqlQuery: String, flattenResults: Boolean): QueryJob = {
    try {
      val sourceTimes = extractTables(sqlQuery)
        .map(t => BigInt(tables.get(t).getLastModifiedTime))
      val temp = Cache.getCacheDestinationTable(sqlQuery).get
      val time = BigInt(tables.get(temp).getLastModifiedTime)
      if (sourceTimes.forall(_ < time)) {
        logger.info(s"Cache hit for query: `$sqlQuery`")
        logger.info(s"Existing destination table: ${bq.BigQueryHelpers.toTableSpec(temp)}")
        QueryJob(sqlQuery, jobReference = None, table = temp)
      } else {
        logger.info(s"Cache invalid for query: `$sqlQuery`")
        val newTemp = tables.createTemporary(
          extractLocation(sqlQuery)
            .getOrElse(tables.DEFAULT_LOCATION))
        logger.info(s"New destination table: ${bq.BigQueryHelpers.toTableSpec(newTemp)}")
        Cache.setCacheDestinationTable(sqlQuery, newTemp)
        delayedQueryJob(sqlQuery, newTemp, flattenResults)
      }
    } catch {
      case NonFatal(e: GoogleJsonResponseException) if isInvalidQuery(e) => throw e
      case NonFatal(_) =>
        val temp = tables.createTemporary(
          extractLocation(sqlQuery)
            .getOrElse(tables.DEFAULT_LOCATION))
        logger.info(s"Cache miss for query: `$sqlQuery`")
        logger.info(s"New destination table: ${bq.BigQueryHelpers.toTableSpec(temp)}")
        Cache.setCacheDestinationTable(sqlQuery, temp)
        delayedQueryJob(sqlQuery, temp, flattenResults)
    }
  }

  private def delayedQueryJob(sqlQuery: String,
                              destinationTable: TableReference,
                              flattenResults: Boolean): QueryJob = {
    val jobReference = {
      val location = extractLocation(sqlQuery).getOrElse(tables.DEFAULT_LOCATION)
      tables.prepareStagingDataset(location)
      val isLegacy = isLegacySql(sqlQuery, flattenResults)
      if (isLegacy) {
        logger.info(s"Executing legacy query: `$sqlQuery`")
      } else {
        logger.info(s"Executing SQL query: `$sqlQuery`")
      }
      val tryRun = run(sqlQuery, destinationTable, flattenResults, isLegacy, dryRun = false)
      Some(tryRun.get.getJobReference)
    }

    QueryJob(sqlQuery, jobReference, destinationTable)
  }

  private val dryRunCache: MMap[(String, Boolean, Boolean), Try[Job]] = MMap.empty

  /* Creates and submits a query job */
  def run(sqlQuery: String,
          destinationTable: String = null,
          flattenResults: Boolean = false): TableReference =
    if (destinationTable != null) {
      val tableRef = bq.BigQueryHelpers.parseTableSpec(destinationTable)
      val queryJob = delayedQueryJob(sqlQuery, tableRef, flattenResults)
      JobService.waitForJobs(projectId, bigquery, queryJob)
      tableRef
    } else {
      val queryJob = newQueryJob(sqlQuery, flattenResults)
      JobService.waitForJobs(projectId, bigquery, queryJob)
      queryJob.table
    }

  /* Creates and submits a query job */
  private def run(sqlQuery: String,
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
      val fullJobId = BigQueryUtil.generateJobId(projectId)
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
    def dryRun(useLegacySql: Boolean): Try[Job] =
      run(sqlQuery, null, flattenResults, useLegacySql, dryRun = true)

    sqlQuery.trim.split("\n")(0).trim.toLowerCase match {
      case "#legacysql"   => true
      case "#standardsql" => false
      case _              =>
        // dry run with SQL syntax first
        dryRun(false) match {
          case Success(_)                                                   => false
          case Failure(e: GoogleJsonResponseException) if isInvalidQuery(e) =>
            // dry run with legacy syntax next
            dryRun(true) match {
              case Success(_) =>
                logger.warn(
                  "Legacy syntax is deprecated, use SQL syntax instead. " +
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
    val tryJob = run(sqlQuery, null, flattenResults = false, isLegacy, dryRun = true)
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
          val l = bigquery.datasets().get(pId, dId).execute().getLocation
          if (l != null) l else tables.DEFAULT_LOCATION
      }
    require(locations.size <= 1, "Tables in the query must be in the same location")
    locations.headOption
  }

}
