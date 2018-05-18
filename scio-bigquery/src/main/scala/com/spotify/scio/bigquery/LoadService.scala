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
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Try

// scalastyle:off parameter.number
private[scio] class LoadService(private val projectId: String,
                                private val bigquery: Bigquery) {

  private val logger = LoggerFactory.getLogger(this.getClass)

  def csv(sources: List[String],
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
          encoding: Option[String] = None): Try[TableReference] = {

    execute(sources = sources, sourceFormat = "CSV", destinationTable = destinationTable,
      createDisposition = createDisposition, writeDisposition = writeDisposition,
      schema = schema, autodetect = Some(autodetect), allowJaggedRows = Some(allowJaggedRows),
      allowQuotedNewLines = Some(allowQuotedNewLines), quote = quote,
      maxBadRecords = maxBadRecords, skipLeadingRows = Some(skipLeadingRows),
      fieldDelimiter = fieldDelimiter,
      ignoreUnknownValues = Some(ignoreUnknownValues), encoding = encoding)
  }

  def json(sources: List[String],
           destinationTable: String,
           createDisposition: CreateDisposition = CREATE_IF_NEEDED,
           writeDisposition: WriteDisposition = WRITE_APPEND,
           schema: Option[TableSchema] = None,
           autodetect: Boolean = false,
           maxBadRecords: Int = 0,
           ignoreUnknownValues: Boolean = false,
           encoding: Option[String] = None): Try[TableReference] = {

    execute(sources = sources, sourceFormat = "NEWLINE_DELIMITED_JSON",
      destinationTable = destinationTable,
      createDisposition = createDisposition, writeDisposition = writeDisposition,
      schema = schema, autodetect = Some(autodetect),
      maxBadRecords = maxBadRecords,
      ignoreUnknownValues = Some(ignoreUnknownValues), encoding = encoding)
  }

  def avro(sources: List[String],
           destinationTable: String,
           createDisposition: CreateDisposition = CREATE_IF_NEEDED,
           writeDisposition: WriteDisposition = WRITE_APPEND,
           schema: Option[TableSchema] = None,
           maxBadRecords: Int = 0,
           encoding: Option[String] = None): Try[TableReference] = {

    execute(sources = sources, sourceFormat = "AVRO",
      destinationTable = destinationTable,
      createDisposition = createDisposition, writeDisposition = writeDisposition,
      schema = schema, maxBadRecords = maxBadRecords, encoding = encoding)
  }

  // scalastyle:off method.length
  private def execute(sources: List[String],
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
                      encoding: Option[String] = None): Try[TableReference] = Try {

    val tableRef = bq.BigQueryHelpers.parseTableSpec(destinationTable)

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

    val fullJobId = BigQueryUtil.generateJobId(projectId)
    val jobReference = new JobReference().setProjectId(projectId).setJobId(fullJobId)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)

    logger.info(s"Loading data into $destinationTable from ${sources.mkString(", ")}")

    bigquery.jobs().insert(projectId, job).execute()

    val loadJob = LoadJob(sources, Some(jobReference), tableRef)

    JobService.waitForJobs(projectId, bigquery, loadJob)

    tableRef
  }

  // scalastyle:on method.length

}
// scalastyle:on parameter.number
