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

package com.spotify.scio
package bigquery
package nio

import com.spotify.scio.bigquery.types.BigQueryType.{HasAnnotation, HasTable, HasQuery}
import com.spotify.scio.nio.ScioIO
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.{AvroBytesUtil, KryoAtomicCoder, KryoOptions}
import com.google.api.services.bigquery.model.{TableRow, TableReference}
import org.apache.beam.sdk.extensions.gcp.options.{GcpOptions, GcsOptions}
import org.apache.beam.sdk.io.gcp.{bigquery => bqio}
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord
import org.apache.beam.sdk.transforms.{Create, DoFn, PTransform, SerializableFunction}
import org.apache.beam.sdk.{io => gio}
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private object Reads {
  private def client(sc: ScioContext) =
    sc.cached[BigQueryClient]{
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
  type WriteP = Nothing // Can't write to a selection query

  def id: String = sqlQuery

  def read(sc: ScioContext, params: ReadParam): SCollection[TableRow] =
    params match {
      case FlattenResults(flattenResults) =>
        Reads.bqReadQuery(sc)(bqio.BigQueryIO.readTableRows(), sqlQuery, flattenResults)
    }

  def tap(read: ReadParam): Tap[TableRow] = ???
  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] = ???
}

object Select {
  sealed trait ReadParam
  final case class FlattenResults(value: Boolean) extends ReadParam
}

/**
   * Get an SCollection for a BigQuery table.
   * @group input
   */
final case class TableRef(table: TableReference) extends ScioIO[TableRow] {
  type ReadP = Unit
  type WriteP = Nothing // TODO

  def id: String = bqio.BigQueryHelpers.toTableSpec(table)

  def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    Reads.bqReadTable(sc)(bqio.BigQueryIO.readTableRows(), table)

  def tap(read: ReadP): Tap[TableRow] = ???
  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] = ???
}

/**
  * Get an SCollection for a BigQuery table.
  * @group input
  */
final case class TableSpec(tableSpec: String) extends ScioIO[TableRow] {
  type ReadP = Unit
  type WriteP = Nothing // TODO

  def id: String = tableSpec

  def read(sc: ScioContext, params: ReadP): SCollection[TableRow] = {
    val ref = bqio.BigQueryHelpers.parseTableSpec(tableSpec)
    TableRef(ref).read(sc, ())
  }

  def tap(read: ReadP): Tap[TableRow] = ???
  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] = ???
}

/**
   * Get an SCollection for a BigQuery TableRow JSON file.
   * @group input
   */
final case class TableRowJsonFile(path: String) extends ScioIO[TableRow] {
  type ReadP = Unit
  type WriteP = Nothing // TODO

  def id: String = path

  def read(sc: ScioContext, params: ReadP): SCollection[TableRow] =
    sc.requireNotClosed {
      sc.wrap(sc.applyInternal(gio.TextIO.read().from(path))).setName(path)
        .map(e => util.ScioUtil.jsonFactory.fromString(e, classOf[TableRow]))
    }

  def tap(read: ReadP): Tap[TableRow] = ???
  def write(data: SCollection[TableRow], params: WriteP): Future[Tap[TableRow]] = ???
}

object Typed {

  /**
   * Get a typed SCollection for a BigQuery table.
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromTable BigQueryType.fromTable]]
   *
   * The source (table) specified in the annotation will be used
   *
   * {{{
   * @BigQueryType.fromTable("publicdata:samples.gsod")
   * class Row
   * }}}
   *
   */
  def apply[T <: HasAnnotation with HasTable : ClassTag : TypeTag]: ScioIO.Aux[T, Unit, Nothing] = {
    val bqt = BigQueryType[T]
    val _table: String = bqt.table.get
    table(_table)
  }

  /**
   * Get a typed SCollection for a BigQuery SELECT query
   *
   * Note that `T` must be annotated with
   * [[com.spotify.scio.bigquery.types.BigQueryType.fromQuery BigQueryType.fromQuery]]
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def apply[T <: HasAnnotation with HasQuery : ClassTag : TypeTag](implicit dummy: DummyImplicit): ScioIO.ReadOnly[T, Unit] = {
    val bqt = BigQueryType[T]
    val _query = bqt.query.get
    query(_query)
  }

  /**
   * Get a typed SCollection for a BigQuery SELECT query
   *
   * Both [[https://cloud.google.com/bigquery/docs/reference/legacy-sql Legacy SQL]] and
   * [[https://cloud.google.com/bigquery/docs/reference/standard-sql/ Standard SQL]] dialects are
   * supported. By default the query dialect will be automatically detected. To override this
   * behavior, start the query string with `#legacysql` or `#standardsql`.
   */
  def query[T <: HasAnnotation : ClassTag : TypeTag](query: String): ScioIO.ReadOnly[T, Unit] =
    new ScioIO[T] {
      type ReadP = Unit
      type WriteP = Nothing // ReadOnly

      def id: String = query
      def read(sc: ScioContext, params: ReadP): SCollection[T] = {
        @inline def typedRead(sc: ScioContext) = Reads.avroBigQueryRead[T](sc)
        Reads.bqReadQuery(sc)(typedRead(sc), query)
      }

      def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = ???
      def tap(read: ReadP): Tap[T] = ???
    }

  /**
   * Get a typed SCollection for a BigQuery table.
   */
  def table[T <: HasAnnotation : ClassTag : TypeTag](table: String): ScioIO.Aux[T, Unit, Nothing] =
    new ScioIO[T] {
      type ReadP = Unit
      type WriteP = Nothing // TODO

      def id: String = table

      def read(sc: ScioContext, params: ReadP): SCollection[T] = {
        @inline def typedRead(sc: ScioContext) = Reads.avroBigQueryRead[T](sc)
        Reads.bqReadTable(sc)(typedRead(sc), bqio.BigQueryHelpers.parseTableSpec(id))
      }

      def write(data: SCollection[T], params: WriteP): Future[Tap[T]] = ???
      def tap(read: ReadP): Tap[T] = ???
    }

  private[scio] def dynamic[T <: HasAnnotation : ClassTag : TypeTag](
    newSource: String
  ): ScioIO.ReadOnly[T, Unit] = {
    val bqt = BigQueryType[T]
    newSource match {
      // newSource is missing, T's companion object must have either table or query
      // The case where newSource is null is only there
      // for legacy support and should not exists once
      // BigQueryScioContext.typedBigQuery is removed
      case null if bqt.isTable =>
        val _table = bqt.table.get
        table(_table)
      case null if bqt.isQuery =>
        val _query = bqt.query.get
        query[T](_query)
      case null =>
        throw new IllegalArgumentException(s"Missing table or query field in companion object")
      case _ =>
        val _table = scala.util.Try(bqio.BigQueryHelpers.parseTableSpec(newSource)).toOption
        if (_table.isDefined) {
          table[T](newSource)
        } else {
          query[T](newSource)
        }
    }
  }
}
