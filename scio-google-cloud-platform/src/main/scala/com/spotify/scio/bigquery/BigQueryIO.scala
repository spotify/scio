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

package com.spotify.scio.bigquery

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders._
import com.spotify.scio.io._
import com.spotify.scio.util.{FilenamePolicySupplier, Functions, ScioUtil}
import com.spotify.scio.values.{SCollection, SideOutput, SideOutputCollections}
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.{Method => ReadMethod}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{
  CreateDisposition,
  Method => WriteMethod,
  WriteDisposition
}
import org.apache.beam.sdk.io.gcp.bigquery._
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple}
import org.joda.time.Duration

import java.util.concurrent.ConcurrentHashMap
import java.util.function
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._
import scala.util.chaining._

private object Reads {

  private[this] val cache = new ConcurrentHashMap[ScioContext, BigQuery]()

  @inline private def client(sc: ScioContext): BigQuery =
    cache.computeIfAbsent(
      sc,
      new function.Function[ScioContext, BigQuery] {
        override def apply(context: ScioContext): BigQuery = {
          val opts = context.optionsAs[GcpOptions]
          BigQuery(opts.getProject, opts.getGcpCredential)
        }
      }
    )

  private[scio] def bqReadQuery[T](sc: ScioContext)(
    typedRead: beam.BigQueryIO.TypedRead[T],
    sqlQuery: String,
    flattenResults: Boolean = false
  ): SCollection[T] = {
    val bigQueryClient = client(sc)
    val labels = sc.labels
    val read = bigQueryClient.query
      .newQueryJob(sqlQuery, flattenResults, labels)
      .map { job =>
        sc.onClose(_ => bigQueryClient.waitForJobs(job))
        typedRead.from(job.table).withoutValidation()
      }

    sc.applyTransform(read.get)
  }

  // TODO: support labels Inheritance like in bqReadQuery
  private[scio] def bqReadStorage[T](sc: ScioContext)(
    typedRead: beam.BigQueryIO.TypedRead[T],
    table: Table,
    selectedFields: List[String] = BigQueryStorage.ReadParam.DefaultSelectFields,
    rowRestriction: Option[String] = BigQueryStorage.ReadParam.DefaultRowRestriction
  ): SCollection[T] = {
    val read = typedRead
      .from(table.spec)
      .withMethod(ReadMethod.DIRECT_READ)
      .withSelectedFields(selectedFields.asJava)
      .pipe(r => rowRestriction.fold(r)(r.withRowRestriction))

    sc.applyTransform(read)
  }
}

private[bigquery] object Writes {
  def resolveMethod(
    method: WriteMethod,
    options: BigQueryOptions,
    isBounded: PCollection.IsBounded
  ): WriteMethod = (method, isBounded) match {
    case (WriteMethod.DEFAULT, _)
        if options.getUseStorageWriteApi && options.getUseStorageWriteApiAtLeastOnce =>
      WriteMethod.STORAGE_API_AT_LEAST_ONCE
    case (WriteMethod.DEFAULT, _) if options.getUseStorageWriteApi =>
      WriteMethod.STORAGE_WRITE_API
    case (WriteMethod.DEFAULT, PCollection.IsBounded.BOUNDED) =>
      WriteMethod.FILE_LOADS
    case (WriteMethod.DEFAULT, PCollection.IsBounded.UNBOUNDED) =>
      WriteMethod.STREAMING_INSERTS
    case _ =>
      method
  }

  def withSharding[T](method: WriteMethod, w: beam.BigQueryIO.Write[T])(
    sharding: Sharding
  ): beam.BigQueryIO.Write[T] = {
    import WriteMethod._
    (sharding, method) match {
      case (Sharding.Auto, _) =>
        w.withAutoSharding()
      case (Sharding.Manual(numShards), FILE_LOADS) =>
        w.withNumFileShards(numShards)
      case (Sharding.Manual(numShards), STORAGE_WRITE_API | STORAGE_API_AT_LEAST_ONCE) =>
        w.withNumStorageWriteApiStreams(numShards)
      case _ =>
        w
    }
  }

  def withSuccessfulInsertsPropagation[T](method: WriteMethod, w: beam.BigQueryIO.Write[T])(
    successfulInsertsPropagation: Boolean
  ): beam.BigQueryIO.Write[T] = {
    import WriteMethod._
    method match {
      case STREAMING_INSERTS =>
        w.withSuccessfulInsertsPropagation(successfulInsertsPropagation)
      case STORAGE_WRITE_API | STORAGE_API_AT_LEAST_ONCE =>
        w.withPropagateSuccessfulStorageApiWrites(successfulInsertsPropagation)
      case _ =>
        w
    }
  }

  def sideOutputs(
    data: SCollection[_],
    method: WriteMethod,
    successfulInsertsPropagation: Boolean,
    extendedErrorInfo: Boolean,
    result: WriteResult
  ): SideOutputCollections = {
    import WriteMethod._
    val sc = data.context
    var tuple = PCollectionTuple.empty(sc.pipeline)
    // success side output
    method match {
      case FILE_LOADS =>
        tuple = tuple.and(BigQueryIO.SuccessfulTableLoads.tupleTag, result.getSuccessfulTableLoads)
      case STREAMING_INSERTS if successfulInsertsPropagation =>
        tuple = tuple.and(BigQueryIO.SuccessfulInserts.tupleTag, result.getSuccessfulInserts)
      case STORAGE_WRITE_API | STORAGE_API_AT_LEAST_ONCE if successfulInsertsPropagation =>
        tuple = tuple.and(
          BigQueryIO.SuccessfulStorageApiInserts.tupleTag,
          result.getSuccessfulStorageApiInserts
        )
      case _ =>
        ()
    }
    // failure side output
    method match {
      case STREAMING_INSERTS if extendedErrorInfo =>
        tuple = tuple.and(BigQueryIO.FailedInsertsWithErr.tupleTag, result.getFailedInsertsWithErr)
      case FILE_LOADS | STREAMING_INSERTS =>
        tuple = tuple.and(BigQueryIO.FailedInserts.tupleTag, result.getFailedInserts)
      case STORAGE_WRITE_API | STORAGE_API_AT_LEAST_ONCE =>
        tuple =
          tuple.and(BigQueryIO.FailedStorageApiInserts.tupleTag, result.getFailedStorageApiInserts)
      case _ =>
        ()
    }

    SideOutputCollections(tuple, sc)
  }

  trait WriteParam[T] {
    def configOverride: beam.BigQueryIO.Write[T] => beam.BigQueryIO.Write[T]
  }

  trait WriteParamDefaults {

    type ConfigOverride[T] = beam.BigQueryIO.Write[T] => beam.BigQueryIO.Write[T]

    val DefaultMethod: WriteMethod = WriteMethod.DEFAULT
    val DefaultSchema: TableSchema = null
    val DefaultWriteDisposition: WriteDisposition = null
    val DefaultCreateDisposition: CreateDisposition = null
    val DefaultTableDescription: String = null
    val DefaultTimePartitioning: TimePartitioning = null
    val DefaultClustering: Clustering = null
    val DefaultTriggeringFrequency: Duration = null
    val DefaultSharding: Sharding = null
    val DefaultFailedInsertRetryPolicy: InsertRetryPolicy = null
    val DefaultSuccessfulInsertsPropagation: Boolean = false
    val DefaultExtendedErrorInfo: Boolean = false
    val DefaultConfigOverride: Null = null
  }
}

sealed trait BigQueryIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, T] = TapOf[T]
}

object BigQueryIO {
  implicit lazy val coderTableDestination: Coder[TableDestination] = Coder.kryo

  lazy val SuccessfulTableLoads: SideOutput[TableDestination] = SideOutput()
  lazy val SuccessfulInserts: SideOutput[TableRow] = SideOutput()
  lazy val SuccessfulStorageApiInserts: SideOutput[TableRow] = SideOutput()

  implicit lazy val coderBigQueryInsertError: Coder[BigQueryInsertError] = Coder.kryo
  implicit lazy val coderBigQueryStorageApiInsertError: Coder[BigQueryStorageApiInsertError] =
    Coder.kryo

  lazy val FailedInserts: SideOutput[TableRow] = SideOutput()
  lazy val FailedInsertsWithErr: SideOutput[BigQueryInsertError] = SideOutput()
  lazy val FailedStorageApiInserts: SideOutput[BigQueryStorageApiInsertError] = SideOutput()

  private[bigquery] val storageWriteMethod =
    Set(WriteMethod.STORAGE_WRITE_API, WriteMethod.STORAGE_API_AT_LEAST_ONCE)

  @inline final def apply[T](id: String): BigQueryIO[T] =
    new BigQueryIO[T] with TestIO[T] {
      override def testId: String = s"BigQueryIO($id)"
    }

  @inline final def apply[T](source: Source): BigQueryIO[T] =
    new BigQueryIO[T] with TestIO[T] {
      override def testId: String = source match {
        case t: Table => s"BigQueryIO(${t.spec})"
        case q: Query => s"BigQueryIO(${q.underlying})"
      }
    }

  @inline final def apply[T](
    id: String,
    selectedFields: List[String],
    rowRestriction: Option[String]
  ): BigQueryIO[T] =
    new BigQueryIO[T] with TestIO[T] {
      override def testId: String =
        s"BigQueryIO($id, List(${selectedFields.mkString(",")}), $rowRestriction)"
    }
}

object BigQueryTypedSelect {
  object ReadParam {
    val DefaultFlattenResults: Boolean = false
  }

  final case class ReadParam private (flattenResults: Boolean = ReadParam.DefaultFlattenResults)
}

final case class BigQueryTypedSelect[T: Coder](
  reader: beam.BigQueryIO.TypedRead[T],
  sqlQuery: Query,
  fromTableRow: TableRow => T
) extends BigQueryIO[T] {
  override type ReadP = BigQueryTypedSelect.ReadParam
  override type WriteP = Nothing // ReadOnly

  override def testId: String = s"BigQueryIO(${sqlQuery.underlying})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val rc = reader.withCoder(coder)
    Reads.bqReadQuery(sc)(rc, sqlQuery.underlying, params.flattenResults)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] =
    throw new UnsupportedOperationException("BigQuerySelect is read-only")

  override def tap(params: ReadP): Tap[T] = {
    val tableReference = BigQuery
      .defaultInstance()
      .query
      .run(sqlQuery.underlying, flattenResults = params.flattenResults)
    BigQueryTap(tableReference).map(fromTableRow)
  }
}

/**
 * Get an SCollection for a BigQuery SELECT query. Both
 * [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
 * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
 * supported. By default the query dialect will be automatically detected. To override this
 * behavior, start the query string with `#legacysql` or `#standardsql`.
 */
final case class BigQuerySelect(sqlQuery: Query) extends BigQueryIO[TableRow] {
  override type ReadP = BigQuerySelect.ReadParam
  override type WriteP = Nothing // ReadOnly

  private[this] lazy val underlying =
    BigQueryTypedSelect(beam.BigQueryIO.readTableRows(), sqlQuery, identity)(coders.tableRowCoder)

  override def testId: String = s"BigQueryIO(${sqlQuery.underlying})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    sc.read(underlying)(params)

  override protected def write(data: SCollection[TableRow], params: WriteP): Tap[TableRow] =
    throw new UnsupportedOperationException("BigQuerySelect is read-only")

  override def tap(params: ReadP): Tap[TableRow] = underlying.tap(params)
}

object BigQuerySelect {
  type ReadParam = BigQueryTypedSelect.ReadParam
  val ReadParam = BigQueryTypedSelect.ReadParam

  @inline final def apply(sqlQuery: String): BigQuerySelect = new BigQuerySelect(Query(sqlQuery))
}

object BigQueryTypedTable {

  /** Defines the format in which BigQuery can be read and written to. */
  sealed abstract class Format[F]
  object Format {
    sealed abstract private[bigquery] class AvroFormat(val useLogicalTypes: Boolean)
        extends Format[GenericRecord]

    case object GenericRecord extends AvroFormat(false)
    case object GenericRecordWithLogicalTypes extends AvroFormat(true)
    case object TableRow extends Format[TableRow]
  }

  final case class WriteParam[T] private (
    method: WriteMethod,
    schema: TableSchema,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition,
    tableDescription: String,
    timePartitioning: TimePartitioning,
    clustering: Clustering,
    triggeringFrequency: Duration,
    sharding: Sharding,
    failedInsertRetryPolicy: InsertRetryPolicy,
    successfulInsertsPropagation: Boolean,
    extendedErrorInfo: Boolean,
    configOverride: WriteParam.ConfigOverride[T]
  ) extends Writes.WriteParam[T]

  object WriteParam extends Writes.WriteParamDefaults {
    @inline final def apply[T](
      method: WriteMethod = DefaultMethod,
      schema: TableSchema = DefaultSchema,
      writeDisposition: WriteDisposition = DefaultWriteDisposition,
      createDisposition: CreateDisposition = DefaultCreateDisposition,
      tableDescription: String = DefaultTableDescription,
      timePartitioning: TimePartitioning = DefaultTimePartitioning,
      clustering: Clustering = DefaultClustering,
      triggeringFrequency: Duration = DefaultTriggeringFrequency,
      sharding: Sharding = DefaultSharding,
      failedInsertRetryPolicy: InsertRetryPolicy = DefaultFailedInsertRetryPolicy,
      successfulInsertsPropagation: Boolean = DefaultSuccessfulInsertsPropagation,
      extendedErrorInfo: Boolean = DefaultExtendedErrorInfo,
      configOverride: ConfigOverride[T] = DefaultConfigOverride
    ): WriteParam[T] = new WriteParam(
      method,
      schema,
      writeDisposition,
      createDisposition,
      tableDescription,
      timePartitioning,
      clustering,
      triggeringFrequency,
      sharding,
      failedInsertRetryPolicy,
      successfulInsertsPropagation,
      extendedErrorInfo,
      configOverride
    )
  }

  private[this] def tableRow(table: Table): BigQueryTypedTable[TableRow] =
    BigQueryTypedTable(
      beam.BigQueryIO.readTableRows(),
      beam.BigQueryIO.writeTableRows(),
      table,
      (r, _) => BigQueryUtils.convertGenericRecordToTableRow(r)
    )(coders.tableRowCoder)

  private[this] def genericRecord(
    table: Table,
    useLogicalTypes: Boolean
  )(implicit c: Coder[GenericRecord]): BigQueryTypedTable[GenericRecord] = {
    BigQueryTypedTable(
      beam.BigQueryIO
        .read(Functions.serializableFn(_.getRecord))
        .pipe(r => if (useLogicalTypes) r.useAvroLogicalTypes() else r),
      beam.BigQueryIO
        .write[GenericRecord]()
        .withAvroFormatFunction(Functions.serializableFn(_.getElement))
        .withAvroSchemaFactory(Functions.serializableFn(BigQueryUtils.toGenericAvroSchema(_, true)))
        .pipe(r => if (useLogicalTypes) r.useAvroLogicalTypes() else r),
      table,
      (genericRecord: GenericRecord, _: TableSchema) => genericRecord
    )
  }

  /**
   * Creates a new instance of [[BigQueryTypedTable]] based on the supplied [[Format]].
   *
   * NOTE: LogicalType support when using `Format.GenericRecord` has some caveats: Reading: Bigquery
   * types DATE, TIME, DATEIME will be read as STRING. Use `Format.GenericRecordWithLogicalTypes`
   * for avro `date`, `timestamp-micros` and `local-timestamp-micros` (avro 1.10+)
   */
  def apply[F: Coder](table: Table, format: Format[F]): BigQueryTypedTable[F] =
    format match {
      case f: Format.AvroFormat => genericRecord(table, f.useLogicalTypes)
      case Format.TableRow      => tableRow(table)
    }

  def apply[T: Coder](
    readerFn: SchemaAndRecord => T,
    writerFn: T => TableRow,
    tableRowFn: TableRow => T,
    table: Table
  ): BigQueryTypedTable[T] = {
    val rFn = Functions.serializableFn(readerFn)
    val wFn = Functions.serializableFn(writerFn)
    val reader = beam.BigQueryIO.read(rFn)
    val writer = beam.BigQueryIO.write[T]().withFormatFunction(wFn)
    val fn: (GenericRecord, TableSchema) => T = (gr, _) =>
      tableRowFn(BigQueryUtils.convertGenericRecordToTableRow(gr))

    BigQueryTypedTable(reader, writer, table, fn)
  }

  def apply[T: Coder](
    readerFn: SchemaAndRecord => T,
    writerFn: T => GenericRecord,
    fn: (GenericRecord, TableSchema) => T,
    table: Table
  ): BigQueryTypedTable[T] = {
    val rFn = Functions.serializableFn(readerFn)
    val wFn = Functions.serializableFn((r: AvroWriteRequest[T]) => writerFn(r.getElement))
    val reader = beam.BigQueryIO.read(rFn)
    val writer = beam.BigQueryIO.write[T]().withAvroFormatFunction(wFn).useAvroLogicalTypes()

    BigQueryTypedTable(reader, writer, table, fn)
  }
}

final case class BigQueryTypedTable[T: Coder](
  reader: beam.BigQueryIO.TypedRead[T],
  writer: beam.BigQueryIO.Write[T],
  table: Table,
  fn: (GenericRecord, TableSchema) => T
) extends BigQueryIO[T]
    with WriteResultIO[T] {
  override type ReadP = Unit
  override type WriteP = BigQueryTypedTable.WriteParam[T]

  override def testId: String = s"BigQueryIO(${table.spec})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val io = reader.from(table.ref).withCoder(coder)
    sc.applyTransform(s"Read BQ table ${table.spec}", io)
  }

  override protected def writeWithResult(
    data: SCollection[T],
    params: WriteP
  ): (Tap[T], SideOutputCollections) = {
    val method = Writes.resolveMethod(
      params.method,
      data.context.optionsAs[BigQueryOptions],
      data.internal.isBounded
    )

    val transform = writer
      .to(table.ref)
      .withMethod(params.method)
      .pipe(w => Option(params.schema).fold(w)(w.withSchema))
      .pipe(w => Option(params.createDisposition).fold(w)(w.withCreateDisposition))
      .pipe(w => Option(params.writeDisposition).fold(w)(w.withWriteDisposition))
      .pipe(w => Option(params.tableDescription).fold(w)(w.withTableDescription))
      .pipe(w => Option(params.timePartitioning).map(_.asJava).fold(w)(w.withTimePartitioning))
      .pipe(w => Option(params.clustering).map(_.asJava).fold(w)(w.withClustering))
      .pipe(w => Option(params.triggeringFrequency).fold(w)(w.withTriggeringFrequency))
      .pipe(w => Option(params.sharding).fold(w)(Writes.withSharding(method, w)))
      .pipe(w =>
        Writes.withSuccessfulInsertsPropagation(method, w)(params.successfulInsertsPropagation)
      )
      .pipe(w => if (params.extendedErrorInfo) w.withExtendedErrorInfo() else w)
      .pipe(w => Option(params.failedInsertRetryPolicy).fold(w)(w.withFailedInsertRetryPolicy))
      .pipe(w => Option(params.configOverride).fold(w)(_(w)))

    val wr = data.applyInternal(transform)
    val outputs = Writes.sideOutputs(
      data,
      method,
      params.successfulInsertsPropagation,
      params.extendedErrorInfo,
      wr
    )

    (tap(()), outputs)
  }

  override def tap(read: ReadP): Tap[T] = BigQueryTypedTap(table, fn)
}

/** Get an IO for a BigQuery table using the storage API. */
final case class BigQueryStorage(
  table: Table,
  selectedFields: List[String],
  rowRestriction: Option[String]
) extends BigQueryIO[TableRow] {
  override type ReadP = Unit
  override type WriteP = Nothing // ReadOnly

  override def testId: String =
    s"BigQueryIO(${table.spec}, List(${selectedFields.mkString(",")}), $rowRestriction)"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[TableRow] = {
    val coder = CoderMaterializer.beam(sc, coders.tableRowCoder)
    val read = beam.BigQueryIO.readTableRows().withCoder(coder)
    Reads.bqReadStorage(sc)(
      read,
      table,
      selectedFields,
      rowRestriction
    )
  }

  override protected def write(data: SCollection[TableRow], params: WriteP): Tap[TableRow] =
    throw new UnsupportedOperationException("BigQueryStorage is read-only")

  override def tap(read: ReadP): Tap[TableRow] = {
    val readOptions = StorageUtil.tableReadOptions(selectedFields, rowRestriction)
    BigQueryStorageTap(table, readOptions)
  }
}

object BigQueryStorage {
  object ReadParam {
    val DefaultSelectFields: List[String] = Nil
    val DefaultRowRestriction: Option[String] = None
  }
}

final case class BigQueryStorageSelect(sqlQuery: Query) extends BigQueryIO[TableRow] {
  override type ReadP = Unit
  override type WriteP = Nothing // ReadOnly

  private[this] lazy val underlying =
    BigQueryTypedSelect(
      beam.BigQueryIO.readTableRows().withMethod(ReadMethod.DIRECT_READ),
      sqlQuery,
      identity
    )(coders.tableRowCoder)

  override def testId: String = s"BigQueryIO(${sqlQuery.underlying})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    sc.read(underlying)(BigQueryTypedSelect.ReadParam())

  override protected def write(data: SCollection[TableRow], params: WriteP): Tap[TableRow] =
    throw new UnsupportedOperationException("BigQuerySelect is read-only")

  override def tap(params: ReadP): Tap[TableRow] = underlying.tap(BigQueryTypedSelect.ReadParam())
}

/** Get an IO for a BigQuery TableRow JSON file. */
final case class TableRowJsonIO(path: String) extends ScioIO[TableRow] {
  override type ReadP = TableRowJsonIO.ReadParam
  override type WriteP = TableRowJsonIO.WriteParam
  override val tapT: TapT.Aux[TableRow, TableRow] = TapOf[TableRow]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    sc.read(TextIO(path))(params)
      .map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))

  override protected def write(data: SCollection[TableRow], params: WriteP): Tap[TableRow] = {
    data
      .map(ScioUtil.jsonFactory.toString)
      .withName("BigQuery write")
      .write(TextIO(path))(params)
    tap(TableRowJsonIO.ReadParam(params))
  }

  override def tap(read: ReadP): Tap[TableRow] =
    TableRowJsonTap(path, read)
}

object TableRowJsonIO {

  type ReadParam = TextIO.ReadParam
  val ReadParam = TextIO.ReadParam

  type WriteParam = TextIO.WriteParam
  object WriteParam {
    val DefaultSuffix: String = ".json"
    val DefaultNumShards: Int = TextIO.WriteParam.DefaultNumShards
    val DefaultCompression: Compression = TextIO.WriteParam.DefaultCompression
    val DefaultFilenamePolicySupplier: FilenamePolicySupplier =
      TextIO.WriteParam.DefaultFilenamePolicySupplier
    val DefaultPrefix: String = TextIO.WriteParam.DefaultPrefix
    val DefaultShardNameTemplate: String = TextIO.WriteParam.DefaultShardNameTemplate
    val DefaultTempDirectory: String = TextIO.WriteParam.DefaultTempDirectory

    def apply(
      suffix: String = DefaultSuffix,
      numShards: Int = DefaultNumShards,
      compression: Compression = DefaultCompression,
      filenamePolicySupplier: FilenamePolicySupplier = DefaultFilenamePolicySupplier,
      prefix: String = DefaultPrefix,
      shardNameTemplate: String = DefaultShardNameTemplate,
      tempDirectory: String = DefaultTempDirectory
    ): WriteParam = {
      TextIO.WriteParam(
        suffix = suffix,
        numShards = numShards,
        compression = compression,
        filenamePolicySupplier = filenamePolicySupplier,
        prefix = prefix,
        shardNameTemplate = shardNameTemplate,
        tempDirectory = tempDirectory
      )
    }
  }

}

object BigQueryTyped {
  import com.spotify.scio.bigquery.{Table => STable}

  @annotation.implicitNotFound(
    """
    Can't find annotation for type ${T}.
    Make sure this class is annotated with BigQueryType.fromStorage, BigQueryType.fromTable or
    BigQueryType.fromQuery.
    Alternatively, use BigQueryTyped.Storage("<table>"), BigQueryTyped.Table("<table>"), or
    BigQueryTyped.Query("<query>") to get a ScioIO instance.
  """
  )
  sealed trait IO[T <: HasAnnotation] {
    type F[_ <: HasAnnotation] <: ScioIO[_]
    def impl: F[T]
  }

  object IO {
    type Aux[T <: HasAnnotation, F0[_ <: HasAnnotation] <: ScioIO[_]] =
      IO[T] { type F[A <: HasAnnotation] = F0[A] }

    implicit def tableIO[T <: HasAnnotation: TypeTag: Coder](implicit
      t: BigQueryType.Table[T]
    ): Aux[T, Table] =
      new IO[T] {
        type F[A <: HasAnnotation] = Table[A]
        def impl: Table[T] = Table(STable.Spec(t.table))
      }

    implicit def queryIO[T <: HasAnnotation: TypeTag: Coder](implicit
      t: BigQueryType.Query[T]
    ): Aux[T, Select] =
      new IO[T] {
        type F[A <: HasAnnotation] = Select[A]
        def impl: Select[T] = Select(Query(t.queryRaw))
      }

    implicit def storageIO[T <: HasAnnotation: TypeTag: Coder](implicit
      t: BigQueryType.StorageOptions[T]
    ): Aux[T, Storage] =
      new IO[T] {
        type F[A <: HasAnnotation] = Storage[A]
        def impl: Storage[T] = Storage(STable.Spec(t.table), Nil, None)
      }
  }

  /**
   * Get a typed SCollection for a BigQuery table or a SELECT query.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromStorage]],
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]], or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]
   *
   * The source (table) specified in the annotation will be used
   */
  @inline final def apply[T <: HasAnnotation](implicit t: IO[T]): t.F[T] =
    t.impl

  /**
   * Get a typed SCollection for a BigQuery SELECT query.
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  final case class Select[T <: HasAnnotation: TypeTag: Coder](query: Query) extends BigQueryIO[T] {
    override type ReadP = Unit
    override type WriteP = Nothing // ReadOnly

    private[this] lazy val underlying = {
      val fromAvro = BigQueryType[T].fromAvro
      val fromTableRow = BigQueryType[T].fromTableRow
      val reader = beam.BigQueryIO
        .read(new SerializableFunction[SchemaAndRecord, T] {
          override def apply(input: SchemaAndRecord): T = fromAvro(input.getRecord)
        })
      BigQueryTypedSelect(reader, query, fromTableRow)
    }

    override def testId: String = s"BigQueryIO(${query.underlying})"

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      sc.read(underlying)(BigQueryTypedSelect.ReadParam())

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] =
      throw new UnsupportedOperationException("Select queries are read-only")

    override def tap(params: ReadP): Tap[T] = underlying.tap(BigQueryTypedSelect.ReadParam())
  }

  object Select {
    @inline final def apply[T <: HasAnnotation: TypeTag: Coder](
      query: String
    ): Select[T] = new Select[T](Query(query))
  }

  /** Get a typed SCollection for a BigQuery table. */
  final case class Table[T <: HasAnnotation: TypeTag: Coder](table: STable)
      extends BigQueryIO[T]
      with WriteResultIO[T] {
    override type ReadP = Unit
    override type WriteP = Table.WriteParam[T]

    private val underlying = {
      val readFn = Functions.serializableFn[SchemaAndRecord, T] { x =>
        BigQueryType[T].fromAvro(x.getRecord)
      }
      val writeFn = Functions.serializableFn[AvroWriteRequest[T], GenericRecord] { x =>
        BigQueryType[T].toAvro(x.getElement)
      }
      val schemaFactory = Functions.serializableFn[TableSchema, org.apache.avro.Schema] { _ =>
        BigQueryType[T].avroSchema
      }
      val parseFn = (r: GenericRecord, _: TableSchema) => BigQueryType[T].fromAvro(r)

      BigQueryTypedTable[T](
        beam.BigQueryIO
          .read(readFn)
          .useAvroLogicalTypes(),
        beam.BigQueryIO
          .write[T]()
          .withAvroFormatFunction(writeFn)
          .withAvroSchemaFactory(schemaFactory)
          .useAvroLogicalTypes(),
        table,
        parseFn
      )
    }

    override def testId: String = s"BigQueryIO(${table.spec})"

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      sc.read(underlying)

    override protected def writeWithResult(
      data: SCollection[T],
      params: WriteP
    ): (Tap[T], SideOutputCollections) = {
      val outputs = data
        .withName(s"${data.tfName}$$Write")
        .write(underlying)(params)
        .outputs
        .get

      (tap(()), outputs)
    }

    override def tap(read: ReadP): Tap[T] =
      BigQueryTypedTap[T](table, underlying.fn)
  }

  object Table {
    final case class WriteParam[T] private (
      method: WriteMethod,
      writeDisposition: WriteDisposition,
      createDisposition: CreateDisposition,
      timePartitioning: TimePartitioning,
      clustering: Clustering,
      triggeringFrequency: Duration,
      sharding: Sharding,
      failedInsertRetryPolicy: InsertRetryPolicy,
      successfulInsertsPropagation: Boolean,
      extendedErrorInfo: Boolean,
      configOverride: WriteParam.ConfigOverride[T]
    ) extends Writes.WriteParam[T]

    object WriteParam extends Writes.WriteParamDefaults {

      @inline final def apply[T](
        method: WriteMethod = DefaultMethod,
        writeDisposition: WriteDisposition = DefaultWriteDisposition,
        createDisposition: CreateDisposition = DefaultCreateDisposition,
        timePartitioning: TimePartitioning = DefaultTimePartitioning,
        clustering: Clustering = DefaultClustering,
        triggeringFrequency: Duration = DefaultTriggeringFrequency,
        sharding: Sharding = DefaultSharding,
        failedInsertRetryPolicy: InsertRetryPolicy = DefaultFailedInsertRetryPolicy,
        successfulInsertsPropagation: Boolean = DefaultSuccessfulInsertsPropagation,
        extendedErrorInfo: Boolean = DefaultExtendedErrorInfo,
        configOverride: ConfigOverride[T] = DefaultConfigOverride
      ): WriteParam[T] = new WriteParam(
        method,
        writeDisposition,
        createDisposition,
        timePartitioning,
        clustering,
        triggeringFrequency,
        sharding,
        failedInsertRetryPolicy,
        successfulInsertsPropagation,
        extendedErrorInfo,
        configOverride
      )

      implicit private[Table] def typedTableWriteParam[T: TypeTag, Info](
        params: Table.WriteParam[T]
      ): BigQueryTypedTable.WriteParam[T] =
        BigQueryTypedTable.WriteParam(
          params.method,
          BigQueryType[T].schema,
          params.writeDisposition,
          params.createDisposition,
          BigQueryType[T].tableDescription.orNull,
          params.timePartitioning,
          params.clustering,
          params.triggeringFrequency,
          params.sharding,
          params.failedInsertRetryPolicy,
          params.successfulInsertsPropagation,
          params.extendedErrorInfo,
          params.configOverride
        )
    }

  }

  /** Get a typed SCollection for a BigQuery table using the storage API. */
  final case class Storage[T <: HasAnnotation: TypeTag: Coder](
    table: STable,
    selectedFields: List[String],
    rowRestriction: Option[String]
  ) extends BigQueryIO[T] {
    override type ReadP = Unit
    override type WriteP = Nothing // ReadOnly

    override def testId: String =
      s"BigQueryIO(${table.spec}, List(${selectedFields.mkString(",")}), $rowRestriction)"

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      val coder = CoderMaterializer.beam(sc, Coder[T])
      val fromAvro = BigQueryType[T].fromAvro
      val reader = beam.BigQueryIO
        .read(new SerializableFunction[SchemaAndRecord, T] {
          override def apply(input: SchemaAndRecord): T = fromAvro(input.getRecord)
        })
        .withCoder(coder)
      Reads.bqReadStorage(sc)(reader, table, selectedFields, rowRestriction)
    }

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] =
      throw new UnsupportedOperationException("Storage API is read-only")

    override def tap(read: ReadP): Tap[T] = {
      val fn = BigQueryType[T].fromTableRow
      val readOptions = StorageUtil.tableReadOptions(selectedFields, rowRestriction)
      BigQueryStorageTap(table, readOptions).map(fn)
    }
  }

  final case class StorageQuery[T <: HasAnnotation: TypeTag: Coder](sqlQuery: Query)
      extends BigQueryIO[T] {
    override type ReadP = Unit
    override type WriteP = Nothing // ReadOnly

    private[this] lazy val underlying = {
      val fromAvro = BigQueryType[T].fromAvro
      val fromTableRow = BigQueryType[T].fromTableRow
      val reader = beam.BigQueryIO
        .read(new SerializableFunction[SchemaAndRecord, T] {
          override def apply(input: SchemaAndRecord): T = fromAvro(input.getRecord)
        })
        .withMethod(ReadMethod.DIRECT_READ)
      BigQueryTypedSelect(reader, sqlQuery, fromTableRow)
    }

    override def testId: String = s"BigQueryIO($sqlQuery)"

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      sc.read(underlying)(BigQueryTypedSelect.ReadParam())

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] =
      throw new UnsupportedOperationException("Storage API is read-only")

    override def tap(read: ReadP): Tap[T] = underlying.tap(BigQueryTypedSelect.ReadParam())
  }

  private[scio] def dynamic[T <: HasAnnotation: TypeTag: Coder](
    newSource: Option[Source]
  ): ScioIO.ReadOnly[T, Unit] = {
    val bqt = BigQueryType[T]
    newSource match {
      // newSource is missing, T's companion object must have either table or query
      // The case where newSource is null is only there
      // for legacy support and should not exists once
      // BigQueryScioContext.typedBigQuery is removed
      case None if bqt.isTable =>
        val table = STable.Spec(bqt.table.get)
        ScioIO.ro[T](Table[T](table))
      case None if bqt.isQuery =>
        val query = Query(bqt.queryRaw.get)
        Select[T](query)
      case Some(s: STable) =>
        ScioIO.ro(Table[T](s))
      case Some(s: Query) =>
        Select[T](s)
      case _ =>
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
    }
  }
}
