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
import com.google.api.services.bigquery.model._
import com.google.cloud.hadoop.util.ApiErrorExtractor
import com.spotify.scio.bigquery.BigQueryClient.Context
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal

private[scio] object TableService {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private[bigquery] val TablePrefix = "scio_query"
  private[bigquery] val TimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")

  private[bigquery] val StagingDatasetPrefix = "scio_bigquery_staging_"
  private[bigquery] val StagingDatasetTableExpirationMs = 86400000L
  private[bigquery] val StagingDatasetDescription = "Staging dataset for temporary tables"

  private[bigquery] val DefaultLocation = "US"
}

private[scio] final case class TableService private (private val ctx: Context) {
  import TableService._

  /** Get rows from a table. */
  def rows(tableSpec: String): Iterator[TableRow] =
    rows(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get rows from a table. */
  def rows(table: TableReference): Iterator[TableRow] =
    new Iterator[TableRow] {
      private val iterator = bq.PatchedBigQueryTableRowIterator.fromTable(table, ctx.client)
      private var _isOpen = false
      private var _hasNext = false

      private def init(): Unit = if (!_isOpen) {
        iterator.open()
        _isOpen = true
        _hasNext = iterator.advance()
      }

      override def hasNext: Boolean = {
        init()
        _hasNext
      }

      override def next(): TableRow = {
        init()
        if (_hasNext) {
          val r = iterator.getCurrent
          _hasNext = iterator.advance()
          r
        } else {
          throw new NoSuchElementException
        }
      }
    }

  /** Get schema from a table. */
  def schema(tableSpec: String): TableSchema =
    schema(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def schema(tableRef: TableReference): TableSchema =
    table(tableRef).getSchema

  /** Get table metadata. */
  def table(tableSpec: String): Table =
    table(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get table metadata. */
  def table(tableRef: TableReference): Table = {
    val p = Option(tableRef.getProjectId).getOrElse(ctx.project)
    ctx.client.tables().get(p, tableRef.getDatasetId, tableRef.getTableId).execute()
  }

  /** Get list of tables in a dataset. */
  def tableReferences(projectId: String, datasetId: String): Seq[TableReference] = {
    val b = Seq.newBuilder[TableReference]
    val req = ctx.client.tables().list(projectId, datasetId)
    var rep = req.execute()
    Option(rep.getTables).foreach(_.asScala.foreach(b += _.getTableReference))
    while (rep.getNextPageToken != null) {
      rep = req.setPageToken(rep.getNextPageToken).execute()
      Option(rep.getTables)
        .foreach(_.asScala.foreach(b += _.getTableReference))
    }
    b.result()
  }

  def create(table: Table): Unit = {
    val options = PipelineOptionsFactory
      .create()
      .as(classOf[BigQueryOptions])
    options.setProject(ctx.project)
    options.setGcpCredential(ctx.credentials)
    try {
      val service = new bq.BigQueryServicesWrapper(options)
      service.createTable(table)
    } finally {
      Option(options.as(classOf[GcsOptions]).getExecutorService)
        .foreach(_.shutdown())
    }
  }

  def create(table: TableReference, schema: TableSchema): Unit =
    create(new Table().setTableReference(table).setSchema(schema))

  def create(tableSpec: String, schema: TableSchema): Unit =
    create(bq.BigQueryHelpers.parseTableSpec(tableSpec), schema)

  /**
   * Check if table exists. Returns `true` if table exists, `false` is table definitely does not
   * exist, throws in other cases (BigQuery exception, network issue etc.).
   */
  def exists(tableRef: TableReference): Boolean =
    try {
      table(tableRef)
      true
    } catch {
      case e: GoogleJsonResponseException
          if e.getDetails.getErrors.get(0).getReason == "notFound" =>
        false
      case e: Throwable => throw e
    }

  /**
   * Check if table exists. Returns `true` if table exists, `false` is table definitely does not
   * exist, throws in other cases (BigQuery exception, network issue etc.).
   */
  def exists(tableSpec: String): Boolean =
    exists(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Write rows to a table. */
  def writeRows(table: TableReference,
                rows: List[TableRow],
                schema: TableSchema,
                writeDisposition: WriteDisposition,
                createDisposition: CreateDisposition): Unit = {
    val options = PipelineOptionsFactory
      .create()
      .as(classOf[BigQueryOptions])
    options.setProject(ctx.project)
    options.setGcpCredential(ctx.credentials)
    try {
      val service = new bq.BigQueryServicesWrapper(options)
      if (createDisposition == CREATE_IF_NEEDED) {
        service.createTable(new Table().setTableReference(table).setSchema(schema))
      }
      service.insertAll(table, rows.asJava)
    } finally {
      Option(options.as(classOf[GcsOptions]).getExecutorService)
        .foreach(_.shutdown())
    }
  }

  /** Write rows to a table. */
  def writeRows(tableSpec: String,
                rows: List[TableRow],
                schema: TableSchema = null,
                writeDisposition: WriteDisposition = WRITE_EMPTY,
                createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit =
    writeRows(bq.BigQueryHelpers.parseTableSpec(tableSpec),
              rows,
              schema,
              writeDisposition,
              createDisposition)

  /** Delete table */
  private[bigquery] def delete(table: TableReference): Unit =
    ctx.client
      .tables()
      .delete(table.getProjectId, table.getDatasetId, table.getTableId)
      .execute()

  /* Create a staging dataset at a specified location, e.g US */
  private[bigquery] def prepareStagingDataset(location: String): Unit = {
    val datasetId = StagingDatasetPrefix + location.toLowerCase
    try {
      ctx.client.datasets().get(ctx.project, datasetId).execute()
      Logger.info(s"Staging dataset ${ctx.project}:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if ApiErrorExtractor.INSTANCE.itemNotFound(e) =>
        Logger.info(s"Creating staging dataset ${ctx.project}:$datasetId")
        val dsRef = new DatasetReference().setProjectId(ctx.project).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(StagingDatasetTableExpirationMs)
          .setDescription(StagingDatasetDescription)
          .setLocation(location)
        ctx.client
          .datasets()
          .insert(ctx.project, ds)
          .execute()
      case NonFatal(e) => throw e
    }
  }

  /* Creates a temporary table in the staging dataset */
  private[bigquery] def createTemporary(location: String): TableReference = {
    val now = Instant.now().toString(TimeFormatter)
    val tableId = TablePrefix + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(ctx.project)
      .setDatasetId(StagingDatasetPrefix + location.toLowerCase)
      .setTableId(tableId)
  }

}
