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

import com.google.api.services.bigquery.model._
import com.google.cloud.storage.{BlobId, BlobInfo, Storage}
import com.spotify.scio.bigquery.client.BigQuery.Client
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.bigquery.{BigQueryType, BigQueryUtil, CREATE_IF_NEEDED, WRITE_APPEND}
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericDatumWriter, GenericRecord}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.apache.commons.lang3.RandomStringUtils
import org.slf4j.LoggerFactory

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._
import scala.util.Try
import scala.reflect.runtime.universe.TypeTag

private[client] object LoadOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)
}

final private[client] class LoadOps(client: Client, jobService: JobOps) {
  import LoadOps._

  def csv(
    sources: List[String],
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
    encoding: Option[String] = None,
    location: Option[String] = None
  ): Try[TableReference] =
    execute(
      sources = sources,
      sourceFormat = "CSV",
      destinationTable = destinationTable,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      schema = schema,
      autodetect = Some(autodetect),
      allowJaggedRows = Some(allowJaggedRows),
      allowQuotedNewLines = Some(allowQuotedNewLines),
      quote = quote,
      maxBadRecords = maxBadRecords,
      skipLeadingRows = Some(skipLeadingRows),
      fieldDelimiter = fieldDelimiter,
      ignoreUnknownValues = Some(ignoreUnknownValues),
      encoding = encoding,
      location = location
    )

  def json(
    sources: List[String],
    destinationTable: String,
    createDisposition: CreateDisposition = CREATE_IF_NEEDED,
    writeDisposition: WriteDisposition = WRITE_APPEND,
    schema: Option[TableSchema] = None,
    autodetect: Boolean = false,
    maxBadRecords: Int = 0,
    ignoreUnknownValues: Boolean = false,
    encoding: Option[String] = None,
    location: Option[String] = None
  ): Try[TableReference] =
    execute(
      sources = sources,
      sourceFormat = "NEWLINE_DELIMITED_JSON",
      destinationTable = destinationTable,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      schema = schema,
      autodetect = Some(autodetect),
      maxBadRecords = maxBadRecords,
      ignoreUnknownValues = Some(ignoreUnknownValues),
      encoding = encoding,
      location = location
    )

  def avro(
    sources: List[String],
    destinationTable: String,
    createDisposition: CreateDisposition = CREATE_IF_NEEDED,
    writeDisposition: WriteDisposition = WRITE_APPEND,
    schema: Option[TableSchema] = None,
    maxBadRecords: Int = 0,
    encoding: Option[String] = None,
    location: Option[String] = None
  ): Try[TableReference] =
    execute(
      sources = sources,
      sourceFormat = "AVRO",
      destinationTable = destinationTable,
      createDisposition = createDisposition,
      writeDisposition = writeDisposition,
      schema = schema,
      maxBadRecords = maxBadRecords,
      encoding = encoding,
      location = location
    )

  /**
   * Upload List of rows to Cloud Storage as Avro file and load to BigQuery table. Note that element
   * type `T` must be annotated with [[BigQueryType]].
   */
  def uploadTypedRows[T <: HasAnnotation: TypeTag](
    tableSpec: String,
    rows: List[T],
    tempLocation: String,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED
  ): Try[TableReference] = {
    val bqt = BigQueryType[T]

    Try {
      val out = new ByteArrayOutputStream()
      val datumWriter = new GenericDatumWriter[GenericRecord]()
      val dataFileWriter = new DataFileWriter[GenericRecord](datumWriter)
      try {
        dataFileWriter.create(bqt.avroSchema, out)
        rows.foreach { row =>
          dataFileWriter.append(bqt.toAvro(row))
        }
      } finally {
        dataFileWriter.close()
      }

      val blobId =
        BlobId.fromGsUtilUri(
          s"${tempLocation.stripSuffix("/")}/upload_${RandomStringUtils.randomAlphanumeric(10)}.avro"
        )
      val blobInfo = BlobInfo.newBuilder(blobId).build
      client.blobStorage.createFrom(
        blobInfo,
        new ByteArrayInputStream(out.toByteArray),
        Storage.BlobWriteOption.doesNotExist(),
        Storage.BlobWriteOption.crc32cMatch()
      )

      blobId
    }.flatMap { blobId =>
      try {
        avro(
          List(blobId.toGsUtilUri),
          tableSpec,
          schema = Some(bqt.schema),
          createDisposition = createDisposition,
          writeDisposition = writeDisposition
        )
      } finally {
        client.blobStorage.delete(blobId)
      }
    }
  }

  @nowarn("msg=private default argument in class LoadOps is never used")
  private def execute(
    sources: List[String],
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
    encoding: Option[String] = None,
    location: Option[String] = None
  ): Try[TableReference] = Try {
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

    val fullJobId = BigQueryUtil.generateJobId(client.project)
    val jobReference = new JobReference()
      .setProjectId(client.project)
      .setJobId(fullJobId)
      .setLocation(location.orNull)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)

    Logger.info(s"Loading data into $destinationTable from ${sources.mkString(", ")}")

    client.execute(_.jobs().insert(client.project, job))

    val loadJob = LoadJob(sources, Some(jobReference), tableRef)

    jobService.waitForJobs(loadJob)

    tableRef
  }
}
