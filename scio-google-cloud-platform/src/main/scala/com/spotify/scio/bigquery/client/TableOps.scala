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
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.google.cloud.bigquery.storage.v1.ReadRowsRequest
import com.google.cloud.bigquery.storage.v1.ReadSession
import com.google.cloud.bigquery.storage.v1.ReadSession.TableReadOptions
import com.google.cloud.hadoop.util.ApiErrorExtractor
import com.spotify.scio.bigquery.client.BigQuery.Client
import com.spotify.scio.bigquery.{BigQuerySysProps, StorageUtil, Table => STable, TableRow}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericRecord}
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryOptions, BigQueryUtils}
import org.apache.beam.sdk.io.gcp.{bigquery => bq}
import org.apache.beam.sdk.options.{ExecutorOptions, PipelineOptionsFactory}
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Random
import scala.util.control.NonFatal

private[client] object TableOps {
  private val Logger = LoggerFactory.getLogger(this.getClass)

  private val TablePrefix = "scio_query"
  private val TimeFormatter = DateTimeFormat.forPattern("yyyyMMddHHmmss")
  private val StagingDatasetPrefix =
    BigQuerySysProps.StagingDatasetPrefix.valueOption.getOrElse("scio_bigquery_staging_")
  private val StagingDatasetTableExpirationMs = 86400000L
  private val StagingDatasetTableMaxTimeTravelHours = 48L
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
    storageAvroRows(table, readOptions).map { gr =>
      BigQueryUtils.convertGenericRecordToTableRow(gr)
    }

  def storageAvroRows(table: STable, readOptions: TableReadOptions): Iterator[GenericRecord] = {
    val tableProjectId = Option(table.ref.getProjectId).getOrElse(client.project)
    val tableUrn =
      s"projects/${tableProjectId}/datasets/${table.ref.getDatasetId}/tables/${table.ref.getTableId}"

    val readSessionProto = ReadSession
      .newBuilder()
      .setTable(tableUrn)
      .setReadOptions(readOptions)
      .setDataFormat(DataFormat.AVRO)

    val request = CreateReadSessionRequest
      .newBuilder()
      .setParent(s"projects/${client.project}")
      .setReadSession(readSessionProto)
      .setMaxStreamCount(1)
      .build()

    val session = client.storage.createReadSession(request)
    val readRowsRequest = ReadRowsRequest
      .newBuilder()
      .setReadStream(session.getStreams(0).getName)
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

      res.iterator
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
    rowRestriction: Option[String] = None
  ): Schema =
    Cache.getOrElse(
      s"""$tableSpec;${selectedFields
          .mkString(",")};$rowRestriction""",
      Cache.SchemaCache
    ) {
      val tableRef = bq.BigQueryHelpers.parseTableSpec(tableSpec)
      val tableProjectId = Option(tableRef.getProjectId).getOrElse(client.project)
      val tableUrn =
        s"projects/${tableProjectId}/datasets/${tableRef.getDatasetId}/tables/${tableRef.getTableId}"

      val readSessionProto = ReadSession
        .newBuilder()
        .setTable(tableUrn)
        .setReadOptions(StorageUtil.tableReadOptions(selectedFields, rowRestriction))
        .setDataFormat(DataFormat.AVRO)

      val request = CreateReadSessionRequest
        .newBuilder()
        .setParent(s"projects/${client.project}")
        .setReadSession(readSessionProto)
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
    client.execute(_.tables().get(p, tableRef.getDatasetId, tableRef.getTableId))
  }

  /** Get list of tables in a dataset. */
  def tableReferences(projectId: String, datasetId: String): Seq[TableReference] =
    tableReferences(Option(projectId), datasetId)

  /** Get list of tables in a dataset. */
  def tableReferences(projectId: Option[String], datasetId: String): Seq[TableReference] = {

    def getNextPage(token: Option[String]): TableList = {
      client.execute { bq =>
        val req = bq.tables().list(projectId.getOrElse(client.project), datasetId)
        token.foreach(req.setPageToken)
        req
      }
    }

    var rep = getNextPage(None)
    val b = Seq.newBuilder[TableReference]
    Option(rep.getTables).foreach(_.asScala.foreach(b += _.getTableReference))
    while (rep.getNextPageToken != null) {
      rep = getNextPage(Some(rep.getNextPageToken))
      Option(rep.getTables)
        .foreach(_.asScala.foreach(b += _.getTableReference))
    }
    b.result()
  }

  def create(table: Table): Unit =
    withBigQueryService(_.createTable(table))

  def create(
    tableRef: TableReference,
    schema: TableSchema,
    description: Option[String] = None
  ): Unit = {
    val table = new Table()
      .setTableReference(tableRef)
      .setSchema(schema)
      .setDescription(description.orNull)
    create(table)
  }

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
    if (
      createDisposition == CreateDisposition.CREATE_IF_NEEDED &&
      service.getTable(tableReference) == null
    ) {
      service.createTable(table)
      // wait creation to be effective before inserting
      Thread.sleep(10.seconds.toMillis)
    }

    writeDisposition match {
      case WriteDisposition.WRITE_TRUNCATE =>
        if (!service.isTableEmpty(tableReference)) {
          delete(tableReference)
          // wait deletion to be effective before re-creating
          Thread.sleep(10.seconds.toMillis)

          service.createTable(table)
          // wait creation to be effective before inserting
          Thread.sleep(10.seconds.toMillis)
        }
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
      Option(options.as(classOf[ExecutorOptions]).getScheduledExecutorService)
        .foreach(_.shutdown())
    }
  }

  /** Delete table */
  private[bigquery] def delete(table: TableReference): Unit = {
    client.execute(
      _.tables()
        .delete(
          Option(table.getProjectId).getOrElse(client.project),
          table.getDatasetId,
          table.getTableId
        )
    )
    ()
  }

  /* Create a staging dataset at a specified location, e.g US */
  private[bigquery] def prepareStagingDataset(location: String): Unit = {
    val datasetId = stagingDatasetId(location)
    try {
      client.execute(_.datasets().get(client.project, datasetId))
      Logger.info(s"Staging dataset ${client.project}:$datasetId already exists")
    } catch {
      case e: GoogleJsonResponseException if ApiErrorExtractor.INSTANCE.itemNotFound(e) =>
        Logger.info(s"Creating staging dataset ${client.project}:$datasetId")
        val dsRef = new DatasetReference().setProjectId(client.project).setDatasetId(datasetId)
        val ds = new Dataset()
          .setDatasetReference(dsRef)
          .setDefaultTableExpirationMs(StagingDatasetTableExpirationMs)
          .setMaxTimeTravelHours(StagingDatasetTableMaxTimeTravelHours)
          .setDescription(StagingDatasetDescription)
          .setLocation(location)
        client.execute(
          _.datasets()
            .insert(client.project, ds)
        )
        ()
      case NonFatal(e) => throw e
    }
  }

  /* Creates a reference to a temporary table in the staging dataset */
  private[bigquery] def createTemporary(location: String): Table =
    createTemporary(temporaryTableReference(location))

  /* Creates a reference to a temporary table in the staging dataset */
  private[bigquery] def createTemporary(tableReference: TableReference): Table =
    new Table()
      .setTableReference(tableReference)
      .setExpirationTime(System.currentTimeMillis() + StagingDatasetTableExpirationMs)

  private[bigquery] def temporaryTableReference(location: String): TableReference = {
    val now = Instant.now().toString(TimeFormatter)
    val rand = Random.nextInt(Int.MaxValue)
    val tableId = TablePrefix + "_" + now + "_" + rand

    new TableReference()
      .setProjectId(client.project)
      .setDatasetId(stagingDatasetId(location))
      .setTableId(tableId)
  }

  private def stagingDatasetId(location: String): String =
    StagingDatasetPrefix + location.toLowerCase.replaceAll("-", "_")
}
