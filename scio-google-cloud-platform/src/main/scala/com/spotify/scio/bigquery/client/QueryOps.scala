/*
 * Copyright 2019 Spotify AB.
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
import com.spotify.scio.bigquery.{Table => STable}
import com.spotify.scio.bigquery.client.BigQuery.{isDML, Client}
import com.spotify.scio.bigquery.{BigQueryUtil, TableRow}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.QueryPriority
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{Map => MMap}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

private[client] object QueryOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private def isInteractive: Boolean = BigQueryConfig.priority.equals(QueryPriority.INTERACTIVE)

  private val Priority = if (isInteractive) "INTERACTIVE" else "BATCH"

  final private case class QueryJobConfig(
    sql: String,
    useLegacySql: Boolean,
    dryRun: Boolean = false,
    destinationTable: TableReference = null,
    flattenResults: Boolean = false,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_EMPTY,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
    labels: Map[String, String] = Map.empty
  )
}

final private[client] class QueryOps(client: Client, tableService: TableOps, jobService: JobOps) {
  import QueryOps._

  /** Get schema for a query without executing it. */
  def schema(sqlQuery: String): TableSchema = Cache.getOrElse(sqlQuery, Cache.SchemaCache) {
    if (isLegacySql(sqlQuery)) {
      // Dry-run not supported for legacy query, using view as a work around
      Logger.info("Getting legacy query schema with view")

      val location = extractLocation(sqlQuery).getOrElse(BigQueryConfig.location)
      tableService.prepareStagingDataset(location)
      val tempTable = tableService.createTemporary(location)
      val tempTableRef = tempTable.getTableReference

      // Create temporary table view and get schema
      Logger.info(s"Creating temporary view ${bq.BigQueryHelpers.toTableSpec(tempTableRef)}")
      val view = new ViewDefinition().setQuery(sqlQuery)
      val viewTable = tempTable.setView(view)
      val schema = client
        .execute(
          _.tables()
            .insert(tempTableRef.getProjectId, tempTableRef.getDatasetId, viewTable)
        )
        .getSchema

      // Delete temporary table
      Logger.info(s"Deleting temporary view ${bq.BigQueryHelpers.toTableSpec(tempTableRef)}")
      client.execute(
        _.tables()
          .delete(tempTableRef.getProjectId, tempTableRef.getDatasetId, tempTableRef.getTableId)
      )

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
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
    labels: Map[String, String] = Map.empty
  ): Iterator[TableRow] = {
    val config = QueryJobConfig(
      sqlQuery,
      useLegacySql = isLegacySql(sqlQuery),
      flattenResults = flattenResults,
      writeDisposition = writeDisposition,
      createDisposition = createDisposition,
      labels = labels
    )

    newQueryJob(config).map { job =>
      jobService.waitForJobs(job)
      tableService.rows(STable.Ref(job.table))
    }.get
  }

  // =======================================================================
  // Query handling
  // =======================================================================
  private[scio] def newQueryJob(
    querySql: String,
    flattenResults: Boolean = false,
    labels: Map[String, String] = Map.empty
  ): Try[QueryJob] = {
    val config = QueryJobConfig(
      querySql,
      useLegacySql = isLegacySql(querySql),
      flattenResults = flattenResults,
      labels = labels
    )

    newQueryJob(config)
  }

  private[scio] def newQueryJob(query: QueryJobConfig): Try[QueryJob] =
    if (BigQueryConfig.isCacheEnabled) {
      newCachedQueryJob(query)
    } else {
      Logger.info(s"BigQuery caching is disabled")

      val location = extractLocation(query.sql).getOrElse(BigQueryConfig.location)
      val tempTable = tableService.createTemporary(location)

      delayedQueryJob(query.copy(destinationTable = tempTable.getTableReference))
    }

  private[scio] def newCachedQueryJob(query: QueryJobConfig): Try[QueryJob] =
    extractTables(query.copy(dryRun = true))
      .flatMap { tableRefs =>
        val sourceTimes = tableRefs
          .map(t => BigInt(tableService.table(t).getLastModifiedTime))

        val temp = Cache.get[TableReference](query.sql, Cache.TableCache).get
        val time = BigInt(tableService.table(temp).getLastModifiedTime)
        if (sourceTimes.forall(_ < time)) {
          Logger.info(s"Cache hit for query: `${query.sql}`")
          Logger.info(s"Existing destination table: ${bq.BigQueryHelpers.toTableSpec(temp)}")

          Success(QueryJob(query.sql, jobReference = None, table = temp))
        } else {
          Logger.info(s"Cache invalid for query: `${query.sql}`")

          val location = extractLocation(query.sql).getOrElse(BigQueryConfig.location)
          val newTemp = tableService.createTemporary(location).getTableReference

          Logger.info(s"New destination table: ${bq.BigQueryHelpers.toTableSpec(newTemp)}")

          Cache.set(query.sql, newTemp, Cache.TableCache)
          delayedQueryJob(query.copy(destinationTable = newTemp))
        }
      }
      .recoverWith {
        case NonFatal(e: GoogleJsonResponseException) if isInvalidQuery(e) => Failure(e)
        case NonFatal(_)                                                   =>
          val temp = tableService
            .createTemporary(
              extractLocation(query.sql)
                .getOrElse(BigQueryConfig.location)
            )
            .getTableReference

          Logger.info(s"Cache miss for query: `${query.sql}`")
          Logger.info(s"New destination table: ${bq.BigQueryHelpers.toTableSpec(temp)}")

          Cache.set(query.sql, temp, Cache.TableCache)
          delayedQueryJob(query.copy(destinationTable = temp))
      }

  private def delayedQueryJob(query: QueryJobConfig): Try[QueryJob] = {
    val location = extractLocation(query.sql).getOrElse(BigQueryConfig.location)
    tableService.prepareStagingDataset(location)
    run(query).map(job => QueryJob(query.sql, Some(job.getJobReference), query.destinationTable))
  }

  private val dryRunCache: MMap[(String, Boolean, Boolean), Try[Job]] = MMap.empty

  /* Creates and submits a query job */
  def run(
    sqlQuery: String,
    destinationTable: String = null,
    flattenResults: Boolean = false,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_EMPTY,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED,
    labels: Map[String, String] = Map.empty
  ): TableReference = {
    val query = QueryJobConfig(
      sqlQuery,
      useLegacySql = isLegacySql(sqlQuery),
      flattenResults = flattenResults,
      writeDisposition = writeDisposition,
      createDisposition = createDisposition,
      labels = labels
    )
    val tableReference = if (destinationTable != null) {
      val tableRef = bq.BigQueryHelpers.parseTableSpec(destinationTable)
      delayedQueryJob(query.copy(destinationTable = tableRef)).map { job =>
        jobService.waitForJobs(job)
        tableRef
      }
    } else {
      newQueryJob(query).map { job =>
        jobService.waitForJobs(job)
        job.table
      }
    }

    tableReference.get
  }

  /* Creates and submits a query job */
  private def run(config: QueryJobConfig): Try[Job] = {
    def run = Try {
      val queryConfig = new JobConfigurationQuery()
        .setQuery(config.sql)
        .setUseLegacySql(config.useLegacySql)
        .setFlattenResults(config.flattenResults)
        .setPriority(Priority)

      Option(config.createDisposition)
        .map(_.name)
        .foreach(queryConfig.setCreateDisposition)
      Option(config.writeDisposition)
        .map(_.name)
        .foreach(queryConfig.setWriteDisposition)

      if (!config.dryRun && !isDML(config.sql)) {
        queryConfig.setAllowLargeResults(true).setDestinationTable(config.destinationTable)
      }

      val jobConfig =
        new JobConfiguration()
          .setQuery(queryConfig)
          .setDryRun(config.dryRun)
          .setLabels(config.labels.asJava)
      val fullJobId = BigQueryUtil.generateJobId(client.project)
      val jobReference = new JobReference().setProjectId(client.project).setJobId(fullJobId)
      val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)
      client.execute(_.jobs().insert(client.project, job))
    }
    if (config.useLegacySql) {
      Logger.info(s"Executing legacy query ($Priority): `${config.sql}`")
    } else {
      Logger.info(s"Executing standard SQL query ($Priority): `${config.sql}`")
    }
    if (config.dryRun) {
      dryRunCache.getOrElseUpdate((config.sql, config.flattenResults, config.useLegacySql), run)
    } else {
      run
    }
  }

  private def isInvalidQuery(e: GoogleJsonResponseException): Boolean =
    Option(e.getDetails)
      .flatMap(details => Option(details.getErrors))
      .flatMap(_.asScala.headOption)
      .exists(_.getReason == "invalidQuery")

  private[scio] def isLegacySql(sqlQuery: String, flattenResults: Boolean = false): Boolean = {
    val dryRun =
      QueryJobConfig(
        sqlQuery,
        dryRun = true,
        useLegacySql = false,
        flattenResults = flattenResults,
        createDisposition = null,
        writeDisposition = null
      )

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
                    "See https://cloud.google.com/bigquery/docs/reference/standard-sql/"
                )
                Logger.warn(s"Legacy query: `$sqlQuery`")
                true
              case Failure(f) =>
                Logger.error(
                  s"Tried both standard and legacy syntax, query `$sqlQuery` failed for both!"
                )
                Logger.error("Standard syntax failed due to:", e)
                Logger.error("Legacy syntax failed due to:", f)
                throw f
            }
          case Failure(e) => throw e
        }
    }
  }

  /** Extract tables to be accessed by a query. */
  def extractTables(sqlQuery: String): Set[TableReference] =
    extractTables(QueryJobConfig(sqlQuery, dryRun = true, useLegacySql = isLegacySql(sqlQuery))).get

  private def extractTables(config: QueryJobConfig): Try[Set[TableReference]] =
    extractTables(run(config))

  private def extractTables(job: Try[Job]): Try[Set[TableReference]] =
    job
      .map(_.getStatistics.getQuery.getReferencedTables)
      .map(Option(_))
      .map(_.map(_.asScala.toSet).getOrElse(Set.empty))

  /** Extract locations of tables to be accessed by a query. */
  def extractLocation(sqlQuery: String): Option[String] = {
    val job = run(QueryJobConfig(sqlQuery, dryRun = true, useLegacySql = isLegacySql(sqlQuery)))
    job.map(_.getJobReference.getLocation) match {
      case Success(l) if l != null => Some(l)
      case _                       =>
        val locations = extractTables(job).get
          .map(t => (t.getProjectId, t.getDatasetId))
          .map { case (pId, dId) =>
            val l = client.execute(_.datasets().get(pId, dId)).getLocation
            if (l != null) l else BigQueryConfig.location
          }
        require(locations.size <= 1, "Tables in the query must be in the same location")
        locations.headOption
    }
  }
}
