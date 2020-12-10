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

import java.util.concurrent.ConcurrentHashMap
import java.util.function

import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.ExtendedErrorInfo._
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders._
import com.spotify.scio.io.{ScioIO, Tap, TapOf, TestIO}
import com.spotify.scio.schemas.{Schema, SchemaMaterializer}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import com.spotify.scio.io.TapT
import com.twitter.chill.ClosureCleaner
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils.ConversionOptions.TruncateTimestamps
import org.apache.beam.sdk.io.gcp.bigquery.{BigQueryUtils, SchemaAndRecord}
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.io.{Compression, TextIO}
import org.apache.beam.sdk.transforms.SerializableFunction

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtilsWrapper

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
    var read = typedRead
      .from(table.spec)
      .withMethod(Method.DIRECT_READ)
      .withSelectedFields(selectedFields.asJava)

    read = rowRestriction.fold(read)(read.withRowRestriction)

    sc.applyTransform(read)
  }
}

private[bigquery] object Writes {
  trait WriteParamDefauls {
    val DefaultSchema: TableSchema = null
    val DefaultWriteDisposition: WriteDisposition = null
    val DefaultCreateDisposition: CreateDisposition = null
    val DefaultTableDescription: String = null
    val DefaultTimePartitioning: TimePartitioning = null
    val DefaultExtendedErrorInfo: ExtendedErrorInfo = ExtendedErrorInfo.Disabled
    def defaultInsertErrorTransform[T <: ExtendedErrorInfo#Info]: SCollection[T] => Unit = sc => {
      // A NoOp on the failed inserts, so that we don't have DropInputs (UnconsumedReads)
      // in the pipeline graph.
      sc.withName("DropFailedInserts").map(_ => ())
      ()
    }
  }
}

sealed trait BigQueryIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, T] = TapOf[T]
}

object BigQueryIO {
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
    private[bigquery] val DefaultFlattenResults = false
  }

  final case class ReadParam private[bigquery] (flattenResults: Boolean = ReadParam.DefaultFlattenResults)
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
    val rc = reader.withCoder(CoderMaterializer.beam(sc, Coder[T]))
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
 * Get an SCollection for a BigQuery SELECT query.
 * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
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
    case object GenericRecord extends Format[GenericRecord]
    case object TableRow extends Format[TableRow]
  }

  trait WriteParam {
    val schema: TableSchema
    val writeDisposition: WriteDisposition
    val createDisposition: CreateDisposition
    val tableDescription: String
    val timePartitioning: TimePartitioning
    val extendedErrorInfo: ExtendedErrorInfo
    val insertErrorTransform: SCollection[extendedErrorInfo.Info] => Unit
  }

  object WriteParam extends Writes.WriteParamDefauls {
    @inline final def apply(
      s: TableSchema,
      wd: WriteDisposition,
      cd: CreateDisposition,
      td: String,
      tp: TimePartitioning,
      ei: ExtendedErrorInfo
    )(it: SCollection[ei.Info] => Unit): WriteParam = new WriteParam {
      val schema: TableSchema = s
      val writeDisposition: WriteDisposition = wd
      val createDisposition: CreateDisposition = cd
      val tableDescription: String = td
      val timePartitioning: TimePartitioning = tp
      val extendedErrorInfo: ei.type = ei
      val insertErrorTransform: SCollection[extendedErrorInfo.Info] => Unit = it
    }

    @inline final def apply(
      s: TableSchema = DefaultSchema,
      wd: WriteDisposition = DefaultWriteDisposition,
      cd: CreateDisposition = DefaultCreateDisposition,
      td: String = DefaultTableDescription,
      tp: TimePartitioning = DefaultTimePartitioning
    ): WriteParam = apply(s, wd, cd, td, tp, DefaultExtendedErrorInfo)(defaultInsertErrorTransform)
  }

  private[this] def tableRow(table: Table): BigQueryTypedTable[TableRow] =
    BigQueryTypedTable(
      beam.BigQueryIO.readTableRows(),
      beam.BigQueryIO.writeTableRows(),
      table,
      BigQueryUtils.convertGenericRecordToTableRow(_, _)
    )(coders.tableRowCoder)

  private[this] def genericRecord(
    table: Table
  )(implicit c: Coder[GenericRecord]): BigQueryTypedTable[GenericRecord] =
    BigQueryTypedTable(
      _.getRecord(),
      identity[GenericRecord],
      (genericRecord: GenericRecord, _: TableSchema) => genericRecord,
      table
    )

  /**
   * Creates a new instance of [[BigQueryTypedTable]] based on the supplied [[Format]].
   *
   * NOTE: LogicalType support when using `Format.GenericRecord` has some caveats:
   * Reading: Bigquery types DATE, TIME, DATIME will be read as STRING
   * Writting: Supports LogicalTypes only for DATE and TIME.
   *           DATETIME is not yet supported. https://issuetracker.google.com/issues/140681683
   */
  def apply[F: Coder](table: Table, format: Format[F]): BigQueryTypedTable[F] =
    format match {
      case Format.GenericRecord => genericRecord(table)(Coder[F]) // Not sure how this compiles since F is unbounded
      case Format.TableRow      => tableRow(table)
    }

  def apply[T: Coder](
    readerFn: SchemaAndRecord => T,
    writerFn: T => TableRow,
    tableRowFn: TableRow => T,
    table: Table
  ): BigQueryTypedTable[T] = {
    val rFn = ClosureCleaner.clean(readerFn)
    val wFn = ClosureCleaner.clean(writerFn)
    val reader = beam.BigQueryIO.read(new SerializableFunction[SchemaAndRecord, T] {
      override def apply(input: SchemaAndRecord): T = rFn(input)
    })
    val writer = beam.BigQueryIO
      .write[T]()
      .withFormatFunction(new SerializableFunction[T, TableRow] {
        override def apply(input: T): TableRow = wFn(input)
      })

    val fn: (GenericRecord, TableSchema) => T = (gr, ts) =>
      tableRowFn(BigQueryUtils.convertGenericRecordToTableRow(gr, ts))

    BigQueryTypedTable(reader, writer, table, fn)
  }

  def apply[T: Coder](
    readerFn: SchemaAndRecord => T,
    writerFn: T => GenericRecord,
    fn: (GenericRecord, TableSchema) => T,
    table: Table
  ): BigQueryTypedTable[T] = {
    val rFn = ClosureCleaner.clean(readerFn)
    val wFn = ClosureCleaner.clean(writerFn)
    val reader = beam.BigQueryIO.read(rFn(_))
    val writer = beam.BigQueryIO
      .write[T]()
      .useAvroLogicalTypes()
      .withAvroFormatFunction(input => wFn(input.getElement()))
      .withAvroSchemaFactory { ts =>
        BigQueryAvroUtilsWrapper.toGenericAvroSchema("root", ts.getFields())
      }

    BigQueryTypedTable(reader, writer, table, fn)
  }
}

final case class BigQueryTypedTable[T: Coder](
  reader: beam.BigQueryIO.TypedRead[T],
  writer: beam.BigQueryIO.Write[T],
  table: Table,
  fn: (GenericRecord, TableSchema) => T
) extends BigQueryIO[T] {
  override type ReadP = Unit
  override type WriteP = BigQueryTypedTable.WriteParam

  override def testId: String = s"BigQueryIO(${table.spec})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val io = reader.from(table.ref).withCoder(CoderMaterializer.beam(sc, Coder[T]))
    sc.applyTransform(s"Read BQ table ${table.spec}", io)
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
    var transform = writer.to(table.ref)
    if (params.schema != null) {
      transform = transform.withSchema(params.schema)
    }
    if (params.createDisposition != null) {
      transform = transform.withCreateDisposition(params.createDisposition)
    }
    if (params.writeDisposition != null) {
      transform = transform.withWriteDisposition(params.writeDisposition)
    }
    if (params.tableDescription != null) {
      transform = transform.withTableDescription(params.tableDescription)
    }
    if (params.timePartitioning != null) {
      transform = transform.withTimePartitioning(params.timePartitioning.asJava)
    }
    transform = params.extendedErrorInfo match {
      case Disabled => transform
      case Enabled  => transform.withExtendedErrorInfo()
    }

    val wr = data.applyInternal(transform)
    params.insertErrorTransform(params.extendedErrorInfo.coll(data.context, wr))

    tap(())
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

  override protected def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    Reads.bqReadStorage(sc)(
      beam.BigQueryIO.readTableRows(),
      table,
      selectedFields,
      rowRestriction
    )

  override protected def write(data: SCollection[TableRow], params: WriteP): Tap[TableRow] =
    throw new UnsupportedOperationException("BigQueryStorage is read-only")

  override def tap(read: ReadP): Tap[TableRow] = {
    val readOptions = StorageUtil.tableReadOptions(selectedFields, rowRestriction)
    BigQueryStorageTap(table, readOptions)
  }
}

object BigQueryStorage {
  object ReadParam {
    private[bigquery] val DefaultSelectFields: List[String] = Nil
    private[bigquery] val DefaultRowRestriction: Option[String] = None
  }
}

final case class BigQueryStorageSelect(sqlQuery: Query) extends BigQueryIO[TableRow] {
  override type ReadP = Unit
  override type WriteP = Nothing // ReadOnly

  private[this] lazy val underlying =
    BigQueryTypedSelect(
      beam.BigQueryIO.readTableRows().withMethod(Method.DIRECT_READ),
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
  override type ReadP = Unit
  override type WriteP = TableRowJsonIO.WriteParam
  final override val tapT: TapT.Aux[TableRow, TableRow] = TapOf[TableRow]

  override protected def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    sc.applyTransform(TextIO.read().from(path))
      .map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))

  override protected def write(data: SCollection[TableRow], params: WriteP): Tap[TableRow] = {
    data.transform_("BigQuery write") {
      _.map(ScioUtil.jsonFactory.toString)
        .applyInternal(data.textOut(path, ".json", params.numShards, params.compression))
    }
    tap(())
  }

  override def tap(read: ReadP): Tap[TableRow] =
    TableRowJsonTap(ScioUtil.addPartSuffix(path))
}

object TableRowJsonIO {
  object WriteParam {
    private[bigquery] val DefaultNumShards = 0
    private[bigquery] val DefaultCompression = Compression.UNCOMPRESSED
  }

  final case class WriteParam private[bigquery] (
    numShards: Int = WriteParam.DefaultNumShards,
    compression: Compression = WriteParam.DefaultCompression
  )
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
  final case class Table[T <: HasAnnotation: TypeTag: Coder](table: STable) extends BigQueryIO[T] {
    private[this] val underlying = {
      val readerFn = BigQueryType[T].fromAvro
      val toTableRow = BigQueryType[T].toTableRow
      val fromTableRow = BigQueryType[T].fromTableRow
      BigQueryTypedTable[T](
        (i: SchemaAndRecord) => readerFn(i.getRecord),
        toTableRow,
        fromTableRow,
        table
      )
    }

    override type ReadP = Unit
    override type WriteP = Table.WriteParam

    override def testId: String = s"BigQueryIO(${table.spec})"

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      sc.read(underlying)

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
      val ps =
        BigQueryTypedTable.WriteParam(
          BigQueryType[T].schema,
          params.writeDisposition,
          params.createDisposition,
          BigQueryType[T].tableDescription.orNull,
          params.timePartitioning,
          params.extendedErrorInfo
        )(params.insertErrorTransform)

      data
        .withName(s"${data.tfName}$$Write")
        .write(underlying)(ps)

      tap(())
    }

    override def tap(read: ReadP): Tap[T] =
      BigQueryTypedTap[T](table, underlying.fn)
  }

  object Table {
    sealed trait WriteParam {
      val writeDisposition: WriteDisposition
      val createDisposition: CreateDisposition
      val timePartitioning: TimePartitioning
      val extendedErrorInfo: ExtendedErrorInfo
      val insertErrorTransform: SCollection[extendedErrorInfo.Info] => Unit
    }

    object WriteParam extends Writes.WriteParamDefauls {
      @inline final def apply(
        wd: WriteDisposition,
        cd: CreateDisposition,
        tp: TimePartitioning,
        ei: ExtendedErrorInfo
      )(it: SCollection[ei.Info] => Unit): WriteParam = new WriteParam {
        val writeDisposition: WriteDisposition = wd
        val createDisposition: CreateDisposition = cd
        val timePartitioning: TimePartitioning = tp
        val extendedErrorInfo: ei.type = ei
        val insertErrorTransform: SCollection[extendedErrorInfo.Info] => Unit = it
      }

      @inline final def apply(
        wd: WriteDisposition = DefaultWriteDisposition,
        cd: CreateDisposition = DefaultCreateDisposition,
        tp: TimePartitioning = DefaultTimePartitioning
      ): WriteParam = apply(wd, cd, tp, DefaultExtendedErrorInfo)(defaultInsertErrorTransform)
    }

  }

  object BeamSchema {
    trait WriteParam {
      val writeDisposition: WriteDisposition
      val createDisposition: CreateDisposition
      val tableDescription: String
      val timePartitioning: TimePartitioning
      val extendedErrorInfo: ExtendedErrorInfo
      val insertErrorTransform: SCollection[extendedErrorInfo.Info] => Unit
    }

    object WriteParam extends Writes.WriteParamDefauls {
      @inline final def apply(
        wd: WriteDisposition,
        cd: CreateDisposition,
        td: String,
        tp: TimePartitioning,
        ei: ExtendedErrorInfo
      )(it: SCollection[ei.Info] => Unit): WriteParam = new WriteParam {
        val writeDisposition: WriteDisposition = wd
        val createDisposition: CreateDisposition = cd
        val tableDescription: String = td
        val timePartitioning: TimePartitioning = tp
        val extendedErrorInfo: ei.type = ei
        val insertErrorTransform: SCollection[extendedErrorInfo.Info] => Unit = it
      }

      @inline final def apply(
        wd: WriteDisposition = DefaultWriteDisposition,
        cd: CreateDisposition = DefaultCreateDisposition,
        td: String = DefaultTableDescription,
        tp: TimePartitioning = DefaultTimePartitioning
      ): WriteParam = apply(wd, cd, td, tp, DefaultExtendedErrorInfo)(defaultInsertErrorTransform)
    }

    def defaultParseFn[T: Schema]: SchemaAndRecord => T = {
      val (schema, _, fromRow) = SchemaMaterializer.materialize(Schema[T])
      input =>
        fromRow {
          BigQueryUtils.toBeamRow(
            input.getRecord,
            schema,
            ConversionOptions.builder().setTruncateTimestamps(TruncateTimestamps.TRUNCATE).build()
          )
        }
    }

    def apply[T: Schema: Coder: ClassTag](table: STable): BeamSchema[T] =
      new BeamSchema(table, defaultParseFn)
  }

  @deprecated("Beam SQL support will be removed in 0.11.0", since = "0.10.1")
  final case class BeamSchema[T: Schema: Coder: ClassTag](
    table: STable,
    parseFn: SchemaAndRecord => T
  ) extends BigQueryIO[T] {
    override type ReadP = Unit
    override type WriteP = BeamSchema.WriteParam

    override def testId: String = s"BigQueryIO(${table.spec})"

    private[this] lazy val underlying: BigQueryTypedTable[T] = {
      val (s, toRow, fromRow) = SchemaMaterializer.materialize(Schema[T])
      BigQueryTypedTable[T](
        parseFn,
        (t: T) => BigQueryUtils.toTableRow(toRow(t)),
        (tr: TableRow) => fromRow(BigQueryUtils.toBeamRow(s, tr)),
        table
      )
    }

    override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
      sc.read(underlying)

    override protected def write(data: SCollection[T], params: WriteP): Tap[T] = {
      val ps = BigQueryTypedTable.WriteParam(
        null,
        params.writeDisposition,
        params.createDisposition,
        params.tableDescription,
        params.timePartitioning,
        params.extendedErrorInfo
      )(params.insertErrorTransform)

      data
        .setSchema(Schema[T])
        .write(underlying.copy(writer = underlying.writer.useBeamSchema()))(ps)
      tap(())
    }

    override def tap(read: ReadP): Tap[T] = BigQueryTypedTap[T](table, underlying.fn)
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
      val fromAvro = BigQueryType[T].fromAvro
      val reader = beam.BigQueryIO
        .read(new SerializableFunction[SchemaAndRecord, T] {
          override def apply(input: SchemaAndRecord): T = fromAvro(input.getRecord)
        })
        .withCoder(CoderMaterializer.beam(sc, Coder[T]))
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
        .withMethod(Method.DIRECT_READ)
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
        val query = Query(bqt.query.get)
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
