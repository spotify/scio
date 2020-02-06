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
import com.google.cloud.bigquery.storage.v1beta1.Storage._
import com.google.cloud.bigquery.storage.v1beta1.TableReferenceProto
import com.google.cloud.bigquery.storage.v1beta1.ReadOptions.TableReadOptions
import com.google.cloud.hadoop.util.ApiErrorExtractor
import com.spotify.scio.bigquery.client.BigQuery.Client
import com.spotify.scio.bigquery.{StorageUtil, TableRow, Table => STable}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtilsWrapper
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._
import scala.util.Random
import scala.util.control.NonFatal

private[client] object TableOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private val TablePrefix = "scio_query"
  private val TimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  private val StagingDatasetPrefix = "scio_bigquery_staging_"
  private val StagingDatasetTableExpirationMs = 86400000L
  private val StagingDatasetDescription = "Staging dataset for temporary tables"
}

final private[client] class TableOps(client: Client) {
  import TableOps._

  /** Get rows from a table. */
  def rows(table: STable): Iterator[TableRow] =
    storageRows(table, TableReadOptions.getDefaultInstance)

  def avroRows(table: STable): Iterator[GenericRecord] =
    storageAvroRows(table, TableReadOptions.getDefaultInstance)

  def storageRows(table: STable, readOptions: TableReadOptions): Iterator[TableRow] =
    withBigQueryService { bqServices =>
      val tb = bqServices.getTable(table.ref, readOptions.getSelectedFieldsList)
      storageAvroRows(table, readOptions).map { gr =>
        BigQueryAvroUtilsWrapper.convertGenericRecordToTableRow(gr, tb.getSchema)
      }
    }

  def storageAvroRows(table: STable, readOptions: TableReadOptions): Iterator[GenericRecord] = {
    val tableRefProto = TableReferenceProto.TableReference
      .newBuilder()
      .setDatasetId(table.ref.getDatasetId)
      .setTableId(table.ref.getTableId)
    if (table.ref.getProjectId != null) {
      tableRefProto.setProjectId(table.ref.getProjectId)
    }

    val request = CreateReadSessionRequest
      .newBuilder()
      .setTableReference(tableRefProto)
      .setReadOptions(readOptions)
      .setParent(s"projects/${client.project}")
      .setRequestedStreams(1)
      .setFormat(DataFormat.AVRO)
      .build()

    val session = client.storage.createReadSession(request)
    val readRowsRequest = ReadRowsRequest
      .newBuilder()
      .setReadPosition(
        StreamPosition
          .newBuilder()
          .setStream(session.getStreams(0))
      )
      .build()

    val schema = new Schema.Parser().parse(session.getAvroSchema.getSchema)
    val reader = new GenericDatumReader[GenericRecord](schema)
    val responses = client.storage.readRowsCallable().call(readRowsRequest).asScala

    var decoder: BinaryDecoder = null
    responses.iterator.flatMap { resp =>
      val bytes = resp.getAvroRows.getSerializedBinaryRows.toByteArray
      decoder = DecoderFactory.get().binaryDecoder(bytes, decoder)

      val res = ArrayBuffer.empty[GenericRecord]
      while (!decoder.isEnd) {
        res += reader.read(null, decoder)
      }

      res.toIterator
    }
  }

  /** Get schema from a table. */
  def schema(tableSpec: String): TableSchema =
    schema(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get schema from a table. */
  def schema(tableRef: TableReference): TableSchema =
    Cache.getOrElse(bq.BigQueryHelpers.toTableSpec(tableRef), Cache.SchemaCache)(
      table(tableRef).getSchema
    )

  /** Get schema from a table using the storage API. */
  def storageReadSchema(
    tableSpec: String,
    selectedFields: List[String] = Nil,
    rowRestriction: String = null
  ): Schema =
    Cache.getOrElse(s"""$tableSpec;${selectedFields
      .mkString(",")};$rowRestriction""", Cache.SchemaCache) {
      val tableRef = bq.BigQueryHelpers.parseTableSpec(tableSpec)
      val tableRefProto = TableReferenceProto.TableReference.newBuilder()
      if (tableRef.getProjectId != null) {
        tableRefProto.setProjectId(tableRef.getProjectId)
      }
      tableRefProto
        .setDatasetId(tableRef.getDatasetId)
        .setTableId(tableRef.getTableId)
        .build()

      val request = CreateReadSessionRequest
        .newBuilder()
        .setTableReference(tableRefProto.build())
        .setReadOptions(StorageUtil.tableReadOptions(selectedFields, rowRestriction))
        .setParent(s"projects/${client.project}")
        .build()
      val session = client.storage.createReadSession(request)
      new Schema.Parser().parse(session.getAvroSchema.getSchema)
    }

  /** Get table metadata. */
  def table(tableSpec: String): Table =
    table(bq.BigQueryHelpers.parseTableSpec(tableSpec))

  /** Get table metadata. */
  def table(tableRef: TableReference): Table = {
    val p = Option(tableRef.getProjectId).getOrElse(client.project)
    client.underlying.tables().get(p, tableRef.getDatasetId, tableRef.getTableId).execute()
  }

  /** Get list of tables in a dataset. */
  def tableReferences(projectId: String, datasetId: String): Seq[TableReference] = {
    val b = Seq.newBuilder[TableReference]
    val req = client.underlying.tables().list(projectId, datasetId)
    var rep = req.execute()
    Option(rep.getTables).foreach(_.asScala.foreach(b += _.getTableReference))
    while (rep.getNextPageToken != null) {
      rep = req.setPageToken(rep.getNextPageToken).execute()
      Option(rep.getTables)
        .foreach(_.asScala.foreach(b += _.getTableReference))
    }
    b.result()
  }

  def create(table: Table): Unit = withBigQueryService(_.createTable(table))

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
  def writeRows(
    tableReference: TableReference,
    rows: List[TableRow],
    schema: TableSchema,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition
  ): Long = withBigQueryService { service =>
    val table = new Table().setTableReference(tableReference).setSchema(schema)
    if (createDisposition == CreateDisposition.CREATE_IF_NEEDED) {
      service.createTable(table)
    }

    writeDisposition match {
      case WriteDisposition.WRITE_TRUNCATE =>
        delete(tableReference)
        service.createTable(table)
      case WriteDisposition.WRITE_EMPTY =>
        require(service.isTableEmpty(tableReference))
      case WriteDisposition.WRITE_APPEND =>
    }

    service.insertAll(tableReference, rows.asJava)
  }

  /** Write rows to a table. */
  def writeRows(
    tableSpec: String,
    rows: List[TableRow],
    schema: TableSchema = null,
    writeDisposition: WriteDisposition = WriteDisposition.WRITE_APPEND,
    createDisposition: CreateDisposition = CreateDisposition.CREATE_IF_NEEDED
  ): Long =
    writeRows(
      bq.BigQueryHelpers.parseTableSpec(tableSpec),
      rows,
      schema,
      writeDisposition,
      createDisposition
    )

  private[bigquery] def withBigQueryService[T](f: bq.BigQueryServicesWrapper => T): T = {
    val options = PipelineOptionsFactory
      .create()
      .as(classOf[BigQueryOptions])
    options.setProject(client.project)
    options.setGcpCredential(client.credentials)

    try {
      f(new bq.BigQueryServicesWrapper(options))
    } finally {
      Option(options.as(classOf[GcsOptions]).getExecutorService)
        .foreach(_.shutdown())
    }
  }

  /** Delete table */
  private[bigquery] def delete(table: TableReference): Unit = {
    client.underlying
      .tables()
      .delete(table.getProjectId, table.getDatasetId, table.getTableId)
      .execute()
    ()
  }

  /* Create a staging dataset at a specified location, e.g US */
  private[bigquery] def prepareStagingDataset(location: String): Unit = {
    val datasetId = stagingDatasetId(location)
    try {
      client.underlying.datasets().get(client.project, datasetId).execute()
      Logger.info(s"Staging dataset ${client.project}:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if ApiErrorExtractor.INSTANCE.itemNotFound(e) =>
        Logger.info(s"Creating staging dataset ${client.project}:$datasetId")
        val dsRef = new DatasetReference().setProjectId(client.project).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(StagingDatasetTableExpirationMs)
          .setDescription(StagingDatasetDescription)
          .setLocation(location)
        client.underlying
          .datasets()
          .insert(client.project, ds)
          .execute()
        ()
      case NonFatal(e) => throw e
    }
  }

  /* Creates a temporary table in the staging dataset */
  private[bigquery] def createTemporary(location: String): TableReference = {
    val now = Instant.now().toString(TimeFormatter)
    val tableId = TablePrefix + "_" + now + "_" + Random.nextInt(Int.MaxValue)
    new TableReference()
      .setProjectId(client.project)
      .setDatasetId(stagingDatasetId(location))
      .setTableId(tableId)
  }

  private def stagingDatasetId(location: String): String =
    StagingDatasetPrefix + location.toLowerCase.replaceAll("-", "_")
}
