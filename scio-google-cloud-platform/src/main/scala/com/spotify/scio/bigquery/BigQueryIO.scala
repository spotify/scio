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
import com.google.cloud.bigquery.storage.v1.DataFormat
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders._
import com.spotify.scio.io._
import com.spotify.scio.util.{FilenamePolicySupplier, Functions, ScioUtil}
import com.spotify.scio.values.{SCollection, SideOutput, SideOutputCollections}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.{Method => ReadMethod}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{
  CreateDisposition,
  Method => WriteMethod,
  WriteDisposition
}
import org.apache.beam.sdk.io.gcp.bigquery._
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.transforms.errorhandling.{BadRecord, ErrorHandler}
import org.apache.beam.sdk.values.{PCollection, PCollectionTuple}
import org.joda.time.Duration

import scala.util.chaining._
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._

case class BigQueryIO[T: Coder](source: Source) extends ScioIO[T] with WriteResultIO[T] {
  import BigQueryIO._

  final override val tapT: TapT.Aux[T, T] = TapOf[T]

  override type ReadP = ReadParam[T]
  override type WriteP = WriteParam[T]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val t = beam.BigQueryIO
      .read(Functions.serializableFn(params.format.parseFn))
      .withCoder(coder)
      .pipe(r => withSource(r)(source))
      .withMethod(params.method)
      .withFormat(params.format.dataFormat)
      .pipe(r => withResultFlattening(r)(params))
      .pipe(r => Option(params.errorHandler).fold(r)(r.withErrorHandler))
      .pipe(r => Option(params.configOverride).fold(r)(_.apply(r)))

    sc.applyTransform(t)
  }

  override protected def writeWithResult(
    data: SCollection[T],
    params: WriteP
  ): (Tap[T], SideOutputCollections) = {
    val method: WriteMethod = resolveMethod(
      params.method,
      data.context.optionsAs[BigQueryOptions],
      data.internal.isBounded
    )
    val t = beam.BigQueryIO
      .write[T]()
      .withMethod(method)
      .pipe(w => withSink(w)(source))
      .pipe(w => withFormatFunction(w)(params.format))
      .pipe(w => Option(params.schema).fold(w)(w.withSchema))
      .pipe(w => Option(params.createDisposition).fold(w)(w.withCreateDisposition))
      .pipe(w => Option(params.writeDisposition).fold(w)(w.withWriteDisposition))
      .pipe(w => Option(params.tableDescription).fold(w)(w.withTableDescription))
      .pipe(w => Option(params.timePartitioning).map(_.asJava).fold(w)(w.withTimePartitioning))
      .pipe(w => Option(params.clustering).map(_.asJava).fold(w)(w.withClustering))
      .pipe(w => Option(params.triggeringFrequency).fold(w)(w.withTriggeringFrequency))
      .pipe(w => Option(params.sharding).fold(w)(withSharding(method, w)))
      .pipe(w => Option(params.failedInsertRetryPolicy).fold(w)(w.withFailedInsertRetryPolicy))
      .pipe(w => withSuccessfulInsertsPropagation(method, w)(params.successfulInsertsPropagation))
      .pipe(w => if (params.extendedErrorInfo) w.withExtendedErrorInfo() else w)
      .pipe(w => Option(params.errorHandler).fold(w)(w.withErrorHandler))
      .pipe(w => Option(params.configOverride).fold(w)(_.apply(w)))

    val wr = data.applyInternal(t)
    val outputs = sideOutputs(
      data,
      method,
      params.successfulInsertsPropagation,
      params.extendedErrorInfo,
      wr
    )

    (tap(ReadParam(params)), outputs)
  }

  override def tap(read: ReadP): Tap[T] = {
    val table = ensureTable(source)
    BigQueryTap(table, read)
  }
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

  @inline def apply[T](id: String): TestIO[T] =
    new TestIO[T] {
      final override val tapT: TapT.Aux[T, T] = TapOf[T]
      override def testId: String = s"BigQueryIO($id)"
    }

  def apply[T <: HasAnnotation: TypeTag: Coder]: BigQueryIO[T] = {
    val bqt = BigQueryType[T]
    val source = if (bqt.isQuery) {
      Query(bqt.queryRaw.get)
    } else if (bqt.isStorage) {
      val selectedFields = bqt.selectedFields
      val rowRestriction = bqt.rowRestriction
      if (selectedFields.isEmpty && rowRestriction.isEmpty) {
        Table(bqt.table.get)
      } else {
        val filter = Table.Filter(selectedFields.getOrElse(Nil), rowRestriction)
        Table(bqt.table.get, filter)
      }
    } else {
      Table(bqt.table.get)
    }
    BigQueryIO(source)
  }

  /** Defines the format in which BigQuery can be read and written to. */
  sealed trait Format[T] extends Serializable {
    type BqType

    def dataFormat: DataFormat

    protected def fromSchemaAndRecord(input: SchemaAndRecord): BqType

    def from(x: BqType): T
    def to(x: T): BqType

    private[bigquery] def parseFn(input: SchemaAndRecord): T =
      from(fromSchemaAndRecord(input))
  }

  object Format {

    class Default[T](_from: TableRow => T, _to: T => TableRow) extends Format[T] {
      override type BqType = TableRow

      override def dataFormat: DataFormat = DataFormat.AVRO

      override def fromSchemaAndRecord(input: SchemaAndRecord): TableRow =
        BigQueryAvroUtilsWrapper.convertGenericRecordToTableRow(
          input.getRecord,
          input.getTableSchema
        )

      override def from(x: TableRow): T = _from(x)
      override def to(x: T): TableRow = _to(x)
    }

    object Default {
      def apply(): Default[TableRow] = new Default(identity, identity)
      def apply[T](from: TableRow => T, to: T => TableRow): Default[T] = new Default(from, to)
      def apply[T](bqt: BigQueryType[T]): Default[T] = new Default(bqt.fromTableRow, bqt.toTableRow)
    }

    class Avro[T](_from: GenericRecord => T, _to: T => GenericRecord) extends Format[T] {
      override type BqType = GenericRecord
      override def dataFormat: DataFormat = DataFormat.AVRO

      override def fromSchemaAndRecord(input: SchemaAndRecord): GenericRecord = input.getRecord
      override def from(x: GenericRecord): T = _from(x)
      override def to(x: T): GenericRecord = _to(x)

      private[bigquery] def formatFunction(x: AvroWriteRequest[T]): GenericRecord = to(x.getElement)
      private[bigquery] def avroSchemaFactory(tableSchema: TableSchema): Schema =
        BigQueryAvroUtilsWrapper.toGenericAvroSchema("root", tableSchema.getFields)
    }

    object Avro {
      def apply(): Avro[GenericRecord] = new Avro(identity, identity)
      def apply[T](from: GenericRecord => T, to: T => GenericRecord): Avro[T] = new Avro(from, to)
      def apply[T](bqt: BigQueryType[T]): Avro[T] = new Avro(bqt.fromAvro, bqt.toAvro)
    }
  }

  object ReadParam {
    type ConfigOverride[T] = beam.BigQueryIO.TypedRead[T] => beam.BigQueryIO.TypedRead[T]

    val DefaultFlattenResults: Boolean = false
    val DefaultErrorHandler: ErrorHandler[BadRecord, _] = null
    val DefaultConfigOverride: Null = null

    private[scio] def apply[T](params: WriteParam[T]): ReadParam[T] = {
      val format = params.format
      // select read method matching with write method
      val method = params.method match {
        case WriteMethod.DEFAULT | WriteMethod.STREAMING_INSERTS => ReadMethod.DEFAULT
        case WriteMethod.FILE_LOADS                              => ReadMethod.EXPORT
        case WriteMethod.STORAGE_WRITE_API | WriteMethod.STORAGE_API_AT_LEAST_ONCE =>
          ReadMethod.DIRECT_READ
      }
      // after write, we'll always read the whole table
      TableReadParam(format, method)
    }
  }

  sealed trait ReadParam[T] {
    def format: Format[T]
    def method: ReadMethod

    def errorHandler: ErrorHandler[BadRecord, _] = ReadParam.DefaultErrorHandler
    def configOverride: ReadParam.ConfigOverride[T] = ReadParam.DefaultConfigOverride
  }

  final case class QueryReadParam[T](
    override val format: Format[T],
    override val method: ReadMethod,
    flattenResults: Boolean = ReadParam.DefaultFlattenResults,
    override val errorHandler: ErrorHandler[BadRecord, _] = ReadParam.DefaultErrorHandler,
    override val configOverride: ReadParam.ConfigOverride[T] = ReadParam.DefaultConfigOverride
  ) extends ReadParam[T]

  final case class TableReadParam[T](
    override val format: Format[T],
    override val method: ReadMethod,
    override val errorHandler: ErrorHandler[BadRecord, _] = ReadParam.DefaultErrorHandler,
    override val configOverride: ReadParam.ConfigOverride[T] = ReadParam.DefaultConfigOverride
  ) extends ReadParam[T]

  object WriteParam {
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
    val DefaultErrorHandler: ErrorHandler[BadRecord, _] = null
    val DefaultConfigOverride: Null = null
  }

  case class WriteParam[T] private (
    format: Format[T],
    method: WriteMethod = WriteParam.DefaultMethod,
    schema: TableSchema = WriteParam.DefaultSchema,
    writeDisposition: WriteDisposition = WriteParam.DefaultWriteDisposition,
    createDisposition: CreateDisposition = WriteParam.DefaultCreateDisposition,
    tableDescription: String = WriteParam.DefaultTableDescription,
    timePartitioning: TimePartitioning = WriteParam.DefaultTimePartitioning,
    clustering: Clustering = WriteParam.DefaultClustering,
    triggeringFrequency: Duration = WriteParam.DefaultTriggeringFrequency,
    sharding: Sharding = WriteParam.DefaultSharding,
    failedInsertRetryPolicy: InsertRetryPolicy = WriteParam.DefaultFailedInsertRetryPolicy,
    successfulInsertsPropagation: Boolean = WriteParam.DefaultSuccessfulInsertsPropagation,
    extendedErrorInfo: Boolean = WriteParam.DefaultExtendedErrorInfo,
    errorHandler: ErrorHandler[BadRecord, _] = WriteParam.DefaultErrorHandler,
    configOverride: WriteParam.ConfigOverride[T] = WriteParam.DefaultConfigOverride
  )

  private[bigquery] def withSource[T](
    r: beam.BigQueryIO.TypedRead[T]
  )(source: Source): beam.BigQueryIO.TypedRead[T] =
    source match {
      case q: Query =>
        r.fromQuery(q.underlying)
          .pipe { r =>
            // TODO dryRun ?
            q.underlying.trim.split("\n")(0).trim.toLowerCase match {
              case "#legacysql"   => r
              case "#standardsql" => r.usingStandardSql()
              case _              => r.usingStandardSql()
            }
          }
      case t: Table =>
        val selectedFields = t.filter.map(_.selectedFields).filter(_.nonEmpty).map(_.asJava)
        val rowRestriction = t.filter.flatMap(_.rowRestriction)
        r
          .from(t.spec)
          .pipe(r => selectedFields.fold(r)(r.withSelectedFields))
          .pipe(r => rowRestriction.fold(r)(r.withRowRestriction))
    }

  private[bigquery] def withResultFlattening[T](
    r: beam.BigQueryIO.TypedRead[T]
  )(param: ReadParam[T]): beam.BigQueryIO.TypedRead[T] = {
    param match {
      case p: QueryReadParam[_] if !p.flattenResults => r.withoutResultFlattening()
      case _                                         => r
    }
  }

  private def ensureTable(source: Source): Table =
    source match {
      case t: Table => t
      case _: Query => throw new IllegalArgumentException("Cannot write with query")
    }

  private[bigquery] def withSink[T](w: beam.BigQueryIO.Write[T])(
    source: Source
  ): beam.BigQueryIO.Write[T] =
    w.to(ensureTable(source).spec)

  private[bigquery] def withFormatFunction[T](
    w: beam.BigQueryIO.Write[T]
  )(format: Format[T]): beam.BigQueryIO.Write[T] = format match {
    case f: Format.Default[T] =>
      w.withFormatFunction(Functions.serializableFn(f.to))
    case f: Format.Avro[T] =>
      w.useAvroLogicalTypes()
        .withAvroFormatFunction(Functions.serializableFn(f.formatFunction))
        .withAvroSchemaFactory(Functions.serializableFn(f.avroSchemaFactory))
  }

  private[bigquery] def withSharding[T](method: WriteMethod, w: beam.BigQueryIO.Write[T])(
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

  private[bigquery] def withSuccessfulInsertsPropagation[T](
    method: WriteMethod,
    w: beam.BigQueryIO.Write[T]
  )(
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

  private[bigquery] def sideOutputs(
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

  private def resolveMethod(
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
