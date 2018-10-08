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

import com.google.api.services.bigquery.Bigquery
import com.google.api.services.bigquery.model._
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

private[scio] object ExtractService {
  private val Logger = LoggerFactory.getLogger(this.getClass)
}

private[scio] final class ExtractService(private val projectId: String,
                                         private val bigquery: Bigquery) {
  import ExtractService._

  def asCsv(sourceTable: String,
            destinationUris: List[String],
            gzipCompression: Boolean = false,
            fieldDelimiter: Option[String] = None,
            printHeader: Option[Boolean] = None): Unit = {

    exportTable(
      sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "CSV",
      gzipCompression = gzipCompression,
      fieldDelimiter = fieldDelimiter,
      printHeader = printHeader
    )
  }

  /** Export a table as Json */
  def asJson(sourceTable: String,
             destinationUris: List[String],
             gzipCompression: Boolean = false): Unit = {

    exportTable(sourceTable = sourceTable,
                destinationUris = destinationUris,
                format = "NEWLINE_DELIMITED_JSON",
                gzipCompression = gzipCompression)
  }

  /** Export a table as Avro */
  def asAvro(sourceTable: String,
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

    val tableRef = bq.BigQueryHelpers.parseTableSpec(sourceTable)

    val jobConfigExtract = new JobConfigurationExtract()
      .setSourceTable(tableRef)
      .setDestinationUris(destinationUris.asJava)
      .setDestinationFormat(format)

    if (gzipCompression) jobConfigExtract.setCompression("GZIP")
    fieldDelimiter.foreach(jobConfigExtract.setFieldDelimiter)
    printHeader.foreach(jobConfigExtract.setPrintHeader(_))

    val jobConfig = new JobConfiguration()
      .setExtract(jobConfigExtract)

    val fullJobId = BigQueryUtil.generateJobId(projectId)
    val jobReference = new JobReference().setProjectId(projectId).setJobId(fullJobId)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)

    Logger.info(s"Extracting table $sourceTable to ${destinationUris.mkString(", ")}")

    bigquery.jobs().insert(projectId, job).execute()

    val extractJob = ExtractJob(destinationUris, Some(jobReference), tableRef)

    JobService.waitForJobs(projectId, bigquery, extractJob)
  }
}
