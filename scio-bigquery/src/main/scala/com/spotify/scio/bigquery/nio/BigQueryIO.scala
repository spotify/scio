/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.bigquery.nio

import com.spotify.scio.util.ScioUtil
import com.spotify.scio.ScioContext
import com.spotify.scio.testing.{BigQueryIO, TableRowJsonIO}
import com.spotify.scio.bigquery.{BigQueryType, BigQueryClient}
import com.spotify.scio.bigquery.types.BigQueryType.{HasAnnotation, HasTable, HasQuery}
import com.spotify.scio.bigquery.io.{BigQueryTap, TableRowJsonTap}
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.{KryoAtomicCoder, KryoOptions}
import com.google.api.services.bigquery.model.{TableRow, TableReference, TableSchema}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.{bigquery => bqio}
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.io.Compression
import org.apache.beam.sdk.{io => gio}
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private object Reads {
  private def client(sc: ScioContext) =
    sc.cached[BigQueryClient] {
      val o = sc.optionsAs[GcpOptions]
      BigQueryClient(o.getProject, o.getGcpCredential)
    }

  private[scio] def bqReadQuery[T: ClassTag](sc: ScioContext)(
                                       typedRead: bqio.BigQueryIO.TypedRead[T],
                                       sqlQuery: String,
                                       flattenResults: Boolean  = false)
  : SCollection[T] = sc.requireNotClosed {
    val bigQueryClient = Reads.client(sc)
    import sc.wrap
    if (bigQueryClient.isCacheEnabled) {
      val queryJob = bigQueryClient.newQueryJob(sqlQuery, flattenResults)

      sc.onClose{ _ =>
        bigQueryClient.waitForJobs(queryJob)
      }

      val read = typedRead.from(queryJob.table).withoutValidation()
      wrap(sc.applyInternal(read)).setName(sqlQuery)
    } else {
      val baseQuery = if (!flattenResults) {
        typedRead.fromQuery(sqlQuery).withoutResultFlattening()
      } else {
        typedRead.fromQuery(sqlQuery)
      }
      val query = if (bigQueryClient.isLegacySql(sqlQuery, flattenResults)) {
        baseQuery
      } else {
        baseQuery.usingStandardSql()
      }
      wrap(sc.applyInternal(query)).setName(sqlQuery)
    }
  }

  private[scio] def avroBigQueryRead[T <: HasAnnotation : ClassTag : TypeTag](sc: ScioContext) = {
    val fn = BigQueryType[T].fromAvro
    bqio.BigQueryIO
      .read(new SerializableFunction[SchemaAndRecord, T] {
        override def apply(input: SchemaAndRecord): T = fn(input.getRecord)
      })
      .withCoder(new KryoAtomicCoder[T](KryoOptions(sc.options)))
  }

  private[scio] def bqReadTable[T: ClassTag](sc: ScioContext)(
    typedRead: bqio.BigQueryIO.TypedRead[T],
    table: TableReference)
  : SCollection[T] = sc.requireNotClosed {
    val tableSpec: String = bqio.BigQueryHelpers.toTableSpec(table)
    sc.wrap(sc.applyInternal(typedRead.from(table))).setName(tableSpec)
  }
}

 /**
  * Get an SCollection for a BigQuery SELECT query.
  * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
  * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
  * supported. By default the query dialect will be automatically detected. To override this
  * behavior, start the query string with `#legacysql` or `#standardsql`.
  * @group input
  */
final case class Select(sqlQuery: String) extends ScioIO[TableRow] {
  import Select._
  type ReadP = ReadParam
  type WriteP = Nothing // ReadOnly

  def id: String = sqlQuery

  def read(sc: ScioContext, params: ReadParam): SCollection[TableRow] =
    params match {
      case FlattenResults(flattenResults) =>
        Reads.bqReadQuery(sc)(bqio.BigQueryIO.readTableRows(), sqlQuery, flattenResults)
    }

  private lazy val bqc = BigQueryClient.defaultInstance()

  def tap(params: ReadParam): Tap[TableRow] =
    params match {
        case FlattenResults(f) =>
          val table = bqc.query(sqlQuery, flattenResults = f)
          BigQueryTap(table)
    }

  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] =
    throw new IllegalStateException("Select queries are read-only")
}

object Select {
  sealed trait ReadParam
  final case class FlattenResults(value: Boolean = false) extends ReadParam
}

/**
 * Get an IO for a BigQuery table.
 * @group input
 */
final case class TableRef(table: TableReference) extends ScioIO[TableRow] {
  type ReadP = Unit
  type WriteP = TableRef.WriteParam

  def id: String = bqio.BigQueryHelpers.toTableSpec(table)

  def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    Reads.bqReadTable(sc)(bqio.BigQueryIO.readTableRows(), table)

  def tap(read: ReadP): Tap[TableRow] =
    BigQueryTap(table)

  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] =
    params match {
      case TableRef.Parameters(schema, writeDisposition, createDisposition, tableDescription) =>
        var transform = bqio.BigQueryIO.writeTableRows().to(table)
        if (schema != null) transform = transform.withSchema(schema)
        if (createDisposition != null) {
          transform = transform.withCreateDisposition(createDisposition)
        }
        if (writeDisposition != null) transform = transform.withWriteDisposition(writeDisposition)
        if (tableDescription != null) transform = transform.withTableDescription(tableDescription)
        data.applyInternal(transform)

        if (writeDisposition == WriteDisposition.WRITE_APPEND) {
          Future.failed(new NotImplementedError("BigQuery future with append not implemented"))
        } else {
          data.context.makeFuture(BigQueryTap(table))
        }
    }
}

object TableRef {
  sealed trait WriteParam
  final case class Parameters(
    schema: TableSchema,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition,
    tableDescription: String) extends WriteParam
}

/**
 * Get an SCollection for a BigQuery table.
 * @group input
 */
final case class TableSpec(tableSpec: String) extends ScioIO[TableRow] {
  type ReadP = Unit
  type WriteP = TableSpec.WriteParam

  def id: String = tableSpec
  private lazy val ref = bqio.BigQueryHelpers.parseTableSpec(tableSpec)

  def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    TableRef(ref).read(sc, params)

  def tap(read: ReadP): Tap[TableRow] =
    TableRef(ref).tap(read)

  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] =
    params match {
      case TableSpec.Parameters(schema, writeDisposition, createDisposition, tableDescription) =>
        val p = TableRef.Parameters(schema, writeDisposition, createDisposition, tableDescription)
        TableRef(ref).write(data, p)
    }

}

object TableSpec {
  sealed trait WriteParam
  final case class Parameters(
    schema: TableSchema,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition,
    tableDescription: String) extends WriteParam
}

/**
 * Get an IO for a BigQuery TableRow JSON file.
 * @group input
 */
final case class TableRowJsonFile(path: String) extends ScioIO[TableRow] {
  type ReadP = Unit
  type WriteP = TableRowJsonFile.Parameters

  def id: String = path

  def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    sc.requireNotClosed {
      sc.wrap(sc.applyInternal(gio.TextIO.read().from(path))).setName(path)
        .map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))
    }

  def tap(read: ReadP): Tap[TableRow] =
    TableRowJsonTap(path)

  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] =
    params match {
      case TableRowJsonFile.Parameters(numShards, compression) =>
        data
          .map(e => ScioUtil.jsonFactory.toString(e))
          .applyInternal(data.textOut(path, ".json", numShards, compression))
        data.context.makeFuture(TableRowJsonTap(ScioUtil.addPartSuffix(path)))
    }
}

object TableRowJsonFile {
  sealed trait WriteParam
  final case class Parameters(
    numShards: Int = 0,
    compression: Compression = Compression.UNCOMPRESSED) extends WriteParam
}

object Typed {
  import scala.language.higherKinds

  @annotation.implicitNotFound("""
    Can't find annotation for type ${T}.
    Make sure this class is annotated with BigQueryType.fromTable or with BigQueryType.fromQuery
    Alternatively, use Typed.Query("<sqlQuery>") or Typed.Table("<bigquery table>")
    to get a ScioIO instance.
  """)
  trait IO[T <: HasAnnotation] {
    type F[_ <: HasAnnotation] <: ScioIO[_]
    def impl: F[T]
  }

  // scalastyle:off structural.type
  object IO {
    type Aux[T <: HasAnnotation, F0[_ <: HasAnnotation] <: ScioIO[_]] =
      IO[T]{ type F[A <: HasAnnotation] = F0[A] }

    implicit def tableIO[T <: HasAnnotation : ClassTag : TypeTag](
      implicit t: BigQueryType.Table[T]): Aux[T, Table] =
        new IO[T] {
          type F[A <: HasAnnotation] = Table[A]
          def impl: Table[T] = Table(t.table)
        }

    implicit def queryIO[T <: HasAnnotation : ClassTag : TypeTag](
      implicit t: BigQueryType.Query[T]): Aux[T, Query] =
        new IO[T] {
          type F[A <: HasAnnotation] = Query[A]
          def impl: Query[T] = Query(t.query)
        }
  }
  // scalastyle:on structural.type

  /**
   * Get a typed SCollection for a BigQuery table or a SELECT query.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]] or
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]
   *
   * The source (table) specified in the annotation will be used
   */
  def apply[T <: HasAnnotation : ClassTag : TypeTag](implicit t: IO[T]): t.F[T] =
    t.impl

  /**
   * Get a typed SCollection for a BigQuery SELECT query
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  case class Query[T <: HasAnnotation : ClassTag : TypeTag](query: String) extends ScioIO[T] {
    type ReadP = Unit
    type WriteP = Nothing // ReadOnly

    def id: String = query
    private lazy val bqt = BigQueryType[T]

    def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      @inline def typedRead(sc: ScioContext) = Reads.avroBigQueryRead[T](sc)
      Reads.bqReadQuery(sc)(typedRead(sc), query)
    }

    def write(data: SCollection[T], params: WriteP): Future[Tap[T]] =
      throw new IllegalStateException("Select queries are read-only")

    def tap(params: ReadP): Tap[T] = {
      Select(query)
        .tap(Select.FlattenResults())
        .map(bqt.fromTableRow)
    }
  }

  /**
   * Get a typed SCollection for a BigQuery table.
   */
  case class Table[T <: HasAnnotation : ClassTag : TypeTag](table: TableReference)
    extends ScioIO[T] {
    type ReadP = Unit
    type WriteP = Table.WriteParam

    def id: String = bqio.BigQueryHelpers.toTableSpec(table)
    private lazy val bqt = BigQueryType[T]

    def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      @inline def typedRead(sc: ScioContext) = Reads.avroBigQueryRead[T](sc)
      Reads.bqReadTable(sc)(typedRead(sc), table)
    }

    def write(data: SCollection[T], params: WriteP): Future[Tap[T]] =
      params match {
        case Table.Parameters(writeDisposition, createDisposition) =>
          val initialTfName = data.tfName
          import scala.concurrent.ExecutionContext.Implicits.global
          val scoll =
            data
              .map(bqt.toTableRow)
              .withName(s"$initialTfName$$Write")

          val ps =
            TableRef.Parameters(
              bqt.schema,
              writeDisposition,
              createDisposition,
              bqt.tableDescription.orNull)

          TableRef(table)
            .write(scoll, ps)
            .map(_.map(bqt.fromTableRow))
      }

    def tap(read: ReadP): Tap[T] =
      TableRef(table)
        .tap(read)
        .map(bqt.fromTableRow)
  }

  object Table {
    sealed trait WriteParam
    final case class Parameters(
      writeDisposition: WriteDisposition,
      createDisposition: CreateDisposition) extends WriteParam

    def apply[T <: HasAnnotation : ClassTag : TypeTag](tableSpec: String): Table[T] =
      Table[T](bqio.BigQueryHelpers.parseTableSpec(tableSpec))
  }

  private[scio] def dynamic[T <: HasAnnotation : ClassTag : TypeTag](
    newSource: String
  ): ScioIO.ReadOnly[T, Unit] = {
    val bqt = BigQueryType[T]
    lazy val table = scala.util.Try(bqio.BigQueryHelpers.parseTableSpec(newSource)).toOption
    newSource match {
      // newSource is missing, T's companion object must have either table or query
      // The case where newSource is null is only there
      // for legacy support and should not exists once
      // BigQueryScioContext.typedBigQuery is removed
      case null if bqt.isTable =>
        val table = bqt.table.get
        ScioIO.ro(Table(table))
      case null if bqt.isQuery =>
        val _query = bqt.query.get
        Query[T](_query)
      case null =>
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      case _ if table.isDefined =>
        ScioIO.ro(Table[T](newSource))
      case _ =>
        Query[T](newSource)
    }
  }
}
