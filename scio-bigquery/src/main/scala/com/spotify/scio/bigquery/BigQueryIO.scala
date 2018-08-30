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

package com.spotify.scio.bigquery

import com.spotify.scio.util.ScioUtil
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.io.{ScioIO, Tap, TestIO}
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.{KryoAtomicCoder, KryoOptions}
import com.google.api.services.bigquery.model.{TableReference, TableSchema}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.{bigquery => beam}
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.apache.beam.sdk.transforms.SerializableFunction
import org.apache.beam.sdk.io.{Compression, TextIO}

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private object Reads {
  private def client(sc: ScioContext) =
    sc.cached[BigQueryClient] {
      val o = sc.optionsAs[GcpOptions]
      BigQueryClient(o.getProject, o.getGcpCredential)
    }

  private[scio] def bqReadQuery[T: ClassTag](sc: ScioContext)
                                            (typedRead: beam.BigQueryIO.TypedRead[T],
                                             sqlQuery: String,
                                             flattenResults: Boolean  = false): SCollection[T] = {
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
    beam.BigQueryIO
      .read(new SerializableFunction[SchemaAndRecord, T] {
        override def apply(input: SchemaAndRecord): T = fn(input.getRecord)
      })
      .withCoder(new KryoAtomicCoder[T](KryoOptions(sc.options)))
  }

  private[scio] def bqReadTable[T: ClassTag](sc: ScioContext)(
    typedRead: beam.BigQueryIO.TypedRead[T],
    table: TableReference)
  : SCollection[T] = {
    val tableSpec: String = beam.BigQueryHelpers.toTableSpec(table)
    sc.wrap(sc.applyInternal(typedRead.from(table))).setName(tableSpec)
  }
}

sealed trait BigQueryIO[T] extends ScioIO[T]

object BigQueryIO {
  def apply[T](id: String): BigQueryIO[T] = new BigQueryIO[T] with TestIO[T] {
    override def testId: String = s"BigQueryIO($id)"
  }
}

/**
 * Get an SCollection for a BigQuery SELECT query.
 * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
 * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
 * supported. By default the query dialect will be automatically detected. To override this
 * behavior, start the query string with `#legacysql` or `#standardsql`.
 */
final case class BigQuerySelect(sqlQuery: String) extends BigQueryIO[TableRow] {
  override type ReadP = BigQuerySelect.ReadParam
  override type WriteP = Nothing // ReadOnly

  private lazy val bqc = BigQueryClient.defaultInstance()

  override def testId: String = s"BigQueryIO($sqlQuery)"

  override def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    Reads.bqReadQuery(sc)(beam.BigQueryIO.readTableRows(), sqlQuery, params.flattenResults)

  override def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] =
    throw new IllegalStateException("BigQuerySelect is read-only")

  override def tap(params: ReadP): Tap[TableRow] =
    BigQueryTap(bqc.query(sqlQuery, flattenResults = params.flattenResults))
}

object BigQuerySelect {
  final case class ReadParam(flattenResults: Boolean = false)
}

/**
 * Get an IO for a BigQuery table.
 */
final case class BigQueryTable(tableSpec: String) extends BigQueryIO[TableRow] {
  override type ReadP = Unit
  override type WriteP = BigQueryTable.WriteParam

  private lazy val table = beam.BigQueryHelpers.parseTableSpec(tableSpec)

  override def testId: String = s"BigQueryIO($tableSpec)"

  override def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    Reads.bqReadTable(sc)(beam.BigQueryIO.readTableRows(), table)

  override def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] = {
    var transform = beam.BigQueryIO.writeTableRows().to(table)
    if (params.schema != null) transform = transform.withSchema(params.schema)
    if (params.createDisposition != null) {
      transform = transform.withCreateDisposition(params.createDisposition)
    }
    if (params.writeDisposition != null) transform = {
      transform.withWriteDisposition(params.writeDisposition)
    }
    if (params.tableDescription != null) transform = {
      transform.withTableDescription(params.tableDescription)
    }
    data.applyInternal(transform)

    if (params.writeDisposition == WriteDisposition.WRITE_APPEND) {
      Future.failed(new NotImplementedError("BigQuery future with append not implemented"))
    } else {
      data.context.makeFuture(BigQueryTap(table))
    }
  }

  override def tap(read: ReadP): Tap[TableRow] = BigQueryTap(table)
}

object BigQueryTable {
  final case class WriteParam(
    schema: TableSchema,
    writeDisposition: WriteDisposition,
    createDisposition: CreateDisposition,
    tableDescription: String)

  def apply(table: TableReference): BigQueryTable = {
    BigQueryTable(beam.BigQueryHelpers.toTableSpec(table))
  }
}

/**
 * Get an IO for a BigQuery TableRow JSON file.
 */
final case class TableRowJsonIO(path: String) extends ScioIO[TableRow] {
  override type ReadP = Unit
  override type WriteP = TableRowJsonIO.WriteParam

  override def read(sc: ScioContext, params: ReadP): SCollection[TableRow] = {
    sc.wrap(sc.applyInternal(TextIO.read().from(path))).setName(path)
      .map(e => ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))
  }

  override def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] = {
    data
      .map(e => ScioUtil.jsonFactory.toString(e))
      .applyInternal(data.textOut(path, ".json", params.numShards, params.compression))
    data.context.makeFuture(tap(Unit))
  }

  override def tap(read: ReadP): Tap[TableRow] = TableRowJsonTap(ScioUtil.addPartSuffix(path))
}

object TableRowJsonIO {
  final case class WriteParam(
    numShards: Int = 0,
    compression: Compression = Compression.UNCOMPRESSED)
}

object BigQueryTyped {
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
      implicit t: BigQueryType.Query[T]): Aux[T, Select] =
        new IO[T] {
          type F[A <: HasAnnotation] = Select[A]
          def impl: Select[T] = Select(t.query)
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
  final case class Select[T <: HasAnnotation : ClassTag : TypeTag](query: String)
    extends BigQueryIO[T] {
    override type ReadP = Unit
    override type WriteP = Nothing // ReadOnly

    private lazy val bqt = BigQueryType[T]

    override def testId: String = s"BigQueryIO($query)"

    override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      @inline def typedRead(sc: ScioContext) = Reads.avroBigQueryRead[T](sc)
      Reads.bqReadQuery(sc)(typedRead(sc), query)
    }

    override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] =
      throw new IllegalStateException("Select queries are read-only")

    override def tap(params: ReadP): Tap[T] =
      com.spotify.scio.bigquery.BigQuerySelect(query)
        .tap(com.spotify.scio.bigquery.BigQuerySelect.ReadParam(flattenResults = false))
        .map(bqt.fromTableRow)
  }

  /**
   * Get a typed SCollection for a BigQuery table.
   */
  final case class Table[T <: HasAnnotation : ClassTag : TypeTag](tableSpec: String)
    extends BigQueryIO[T] {
    override type ReadP = Unit
    override type WriteP = Table.WriteParam

    private lazy val bqt = BigQueryType[T]
    private lazy val table = beam.BigQueryHelpers.parseTableSpec(tableSpec)

    override def testId: String = s"BigQueryIO($tableSpec)"

    override def read(sc: ScioContext, params: ReadP): SCollection[T] = {
      @inline def typedRead(sc: ScioContext) = Reads.avroBigQueryRead[T](sc)
      Reads.bqReadTable(sc)(typedRead(sc), table)
    }

    override def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = {
      val initialTfName = data.tfName
      import scala.concurrent.ExecutionContext.Implicits.global
      val rows =
        data
          .map(bqt.toTableRow)
          .withName(s"$initialTfName$$Write")

      val ps =
        BigQueryTable.WriteParam(
          bqt.schema,
          params.writeDisposition,
          params.createDisposition,
          bqt.tableDescription.orNull)

      BigQueryTable(table)
        .write(rows, ps)
        .map(_.map(bqt.fromTableRow))
    }

    override def tap(read: ReadP): Tap[T] =
      BigQueryTable(table)
        .tap(read)
        .map(bqt.fromTableRow)
  }

  object Table {
    final case class WriteParam(
      writeDisposition: WriteDisposition,
      createDisposition: CreateDisposition)

    def apply[T <: HasAnnotation : ClassTag : TypeTag](table: TableReference): Table[T] =
      Table[T](beam.BigQueryHelpers.toTableSpec(table))
  }

  private[scio] def dynamic[T <: HasAnnotation : ClassTag : TypeTag](
    newSource: String
  ): ScioIO.ReadOnly[T, Unit] = {
    val bqt = BigQueryType[T]
    lazy val table = scala.util.Try(beam.BigQueryHelpers.parseTableSpec(newSource)).toOption
    newSource match {
      // newSource is missing, T's companion object must have either table or query
      // The case where newSource is null is only there
      // for legacy support and should not exists once
      // BigQueryScioContext.typedBigQuery is removed
      case null if bqt.isTable =>
        val table = bqt.table.get
        ScioIO.ro[T](Table(table))
      case null if bqt.isQuery =>
        val _query = bqt.query.get
        Select[T](_query)
      case null =>
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      case _ if table.isDefined =>
        ScioIO.ro(Table[T](newSource))
      case _ =>
        Select[T](newSource)
    }
  }
}
