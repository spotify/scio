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
import com.spotify.scio.bigquery.client.BigQuery.Client
import com.spotify.scio.bigquery.BigQueryUtil
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

private[client] object ExtractOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)
}

sealed trait CompressionT {
  class Compression(val name: Option[String])
}

sealed trait GzipT extends CompressionT {
  case class Gzip() extends Compression(Some("GZIP"))
}

sealed trait DeflateT extends CompressionT {
  case class Deflate() extends Compression(Some("DEFLATE"))
}

sealed trait SnappyT extends CompressionT {
  case class Snappy() extends Compression(Some("SNAPPY"))
}

sealed trait NoCompressionT extends CompressionT {
  case class NoCompression() extends Compression(None)
}

object CsvCompression extends NoCompressionT with GzipT
object AvroCompression extends NoCompressionT with DeflateT with SnappyT
object JsonCompression extends NoCompressionT with GzipT

final private[client] class ExtractOps(client: Client, jobService: JobOps) {
  import ExtractOps._

  def asCsv(
    sourceTable: String,
    destinationUris: List[String],
    compression: CsvCompression.Compression = CsvCompression.NoCompression(),
    fieldDelimiter: Option[String] = None,
    printHeader: Option[Boolean] = None
  ): Unit =
    exportTable(
      sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "CSV",
      compression = compression.name,
      fieldDelimiter = fieldDelimiter,
      printHeader = printHeader
    )

  /** Export a table as Json */
  def asJson(
    sourceTable: String,
    destinationUris: List[String],
    compression: JsonCompression.Compression = JsonCompression.NoCompression()
  ): Unit =
    exportTable(
      sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "NEWLINE_DELIMITED_JSON",
      compression = compression.name
    )

  /** Export a table as Avro */
  def asAvro(
    sourceTable: String,
    destinationUris: List[String],
    compression: AvroCompression.Compression = AvroCompression.NoCompression()
  ): Unit =
    exportTable(
      sourceTable = sourceTable,
      destinationUris = destinationUris,
      format = "AVRO",
      compression = compression.name
    )

  private def exportTable(
    sourceTable: String,
    destinationUris: List[String],
    format: String,
    compression: Option[String],
    fieldDelimiter: Option[String] = None,
    printHeader: Option[Boolean] = None
  ): Unit = {
    val tableRef = bq.BigQueryHelpers.parseTableSpec(sourceTable)

    val jobConfigExtract = new JobConfigurationExtract()
      .setSourceTable(tableRef)
      .setDestinationUris(destinationUris.asJava)
      .setDestinationFormat(format)

    compression.foreach(jobConfigExtract.setCompression)
    fieldDelimiter.foreach(jobConfigExtract.setFieldDelimiter)
    printHeader.foreach(jobConfigExtract.setPrintHeader(_))

    val jobConfig = new JobConfiguration().setExtract(jobConfigExtract)

    val fullJobId = BigQueryUtil.generateJobId(client.project)
    val jobReference = new JobReference().setProjectId(client.project).setJobId(fullJobId)
    val job = new Job().setConfiguration(jobConfig).setJobReference(jobReference)

    Logger.info(s"Extracting table $sourceTable to ${destinationUris.mkString(", ")}")

    client.execute(_.jobs().insert(client.project, job))

    val extractJob = ExtractJob(destinationUris, Some(jobReference), tableRef)

    jobService.waitForJobs(extractJob)
  }
}
