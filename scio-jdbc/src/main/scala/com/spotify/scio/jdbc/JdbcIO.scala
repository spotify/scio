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

package com.spotify.scio.jdbc

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.jdbc.JdbcIO.{
  PreparedStatementSetter,
  ReadWithPartitions,
  StatementPreparator
}
import org.apache.beam.sdk.io.jdbc.{JdbcIO => BJdbcIO}
import org.apache.beam.sdk.values.{TypeDescriptor, TypeDescriptors}
import org.joda.time.{DateTime, Duration}

import java.sql.{PreparedStatement, ResultSet, SQLException}
import javax.sql.DataSource
import scala.util.chaining._

sealed trait JdbcIO[T] extends ScioIO[T]

object JdbcIO {

  @deprecated("Use new API overloads with multiple parameters", since = "0.13.0")
  final def apply[T](opts: JdbcIoOptions): JdbcIO[T] =
    opts match {
      case readOpts: JdbcReadOptions[_]   => apply(readOpts.connectionOptions, readOpts.query)
      case writeOpts: JdbcWriteOptions[_] => apply(writeOpts.connectionOptions, writeOpts.statement)
    }

  final def apply[T](opts: JdbcConnectionOptions, query: String): JdbcIO[T] =
    new JdbcIO[T] with TestIO[T] {
      final override val tapT = EmptyTapOf[T]
      override def testId: String = s"JdbcIO(${jdbcIoId(opts, query)})"
    }

  private[jdbc] def jdbcIoId(opts: JdbcConnectionOptions, query: String): String = {
    val user = opts.password
      .fold(s"${opts.username}")(password => s"${opts.username}:$password")
    s"$user@${opts.connectionUrl}:$query"
  }

  private[jdbc] def dataSourceConfiguration(
    opts: JdbcConnectionOptions
  ): BJdbcIO.DataSourceConfiguration =
    opts.password match {
      case Some(pass) =>
        BJdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
          .withPassword(pass)
      case None =>
        BJdbcIO.DataSourceConfiguration
          .create(opts.driverClass.getCanonicalName, opts.connectionUrl)
          .withUsername(opts.username)
    }

  object ReadParam {
    val BeamDefaultFetchSize: Int = -1
    val DefaultOutputParallelization: Boolean = true
    val DefaultStatementPreparator: PreparedStatement => Unit = null
    val DefaultDataSourceProviderFn: () => DataSource = null
    def defaultConfigOverride[T]: BJdbcIO.Read[T] => BJdbcIO.Read[T] = identity
  }

  final case class ReadParam[T] private (
    rowMapper: ResultSet => T,
    statementPreparator: PreparedStatement => Unit = ReadParam.DefaultStatementPreparator,
    fetchSize: Int = ReadParam.BeamDefaultFetchSize,
    outputParallelization: Boolean = ReadParam.DefaultOutputParallelization,
    dataSourceProviderFn: () => DataSource = ReadParam.DefaultDataSourceProviderFn,
    configOverride: BJdbcIO.Read[T] => BJdbcIO.Read[T] = ReadParam.defaultConfigOverride[T]
  )

  object WriteParam {
    val BeamDefaultBatchSize: Long = -1L
    val BeamDefaultMaxRetryAttempts: Int = 5
    val BeamDefaultInitialRetryDelay: Duration = org.joda.time.Duration.ZERO
    val BeamDefaultMaxRetryDelay: Duration = org.joda.time.Duration.ZERO
    val BeamDefaultRetryConfiguration: BJdbcIO.RetryConfiguration =
      BJdbcIO.RetryConfiguration.create(
        BeamDefaultMaxRetryAttempts,
        BeamDefaultMaxRetryDelay,
        BeamDefaultInitialRetryDelay
      )
    val DefaultRetryStrategy: SQLException => Boolean =
      new BJdbcIO.DefaultRetryStrategy().apply
    val DefaultAutoSharding: Boolean = false
    val DefaultDataSourceProviderFn: () => DataSource = null
    def defaultConfigOverride[T]: BJdbcIO.Write[T] => BJdbcIO.Write[T] = identity
  }

  final case class WriteParam[T] private (
    preparedStatementSetter: (T, PreparedStatement) => Unit,
    batchSize: Long = WriteParam.BeamDefaultBatchSize,
    retryConfiguration: BJdbcIO.RetryConfiguration = WriteParam.BeamDefaultRetryConfiguration,
    retryStrategy: SQLException => Boolean = WriteParam.DefaultRetryStrategy,
    autoSharding: Boolean = WriteParam.DefaultAutoSharding,
    dataSourceProviderFn: () => DataSource = WriteParam.DefaultDataSourceProviderFn,
    configOverride: BJdbcIO.Write[T] => BJdbcIO.Write[T] = WriteParam.defaultConfigOverride[T]
  )
}

final case class JdbcSelect[T: Coder](opts: JdbcConnectionOptions, query: String)
    extends JdbcIO[T] {
  override type ReadP = JdbcIO.ReadParam[T]
  override type WriteP = Nothing
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(opts, query)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val transform = BJdbcIO
      .read[T]()
      .withCoder(coder)
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(opts))
      .withQuery(query)
      .withRowMapper(params.rowMapper(_))
      .withOutputParallelization(params.outputParallelization)
      .pipe { r =>
        Option(params.dataSourceProviderFn)
          .map(fn => Functions.serializableFn[Void, DataSource](_ => fn()))
          .fold(r)(r.withDataSourceProviderFn)
      }
      .pipe { r =>
        Option(params.statementPreparator)
          .map[StatementPreparator](fn => fn(_))
          .fold(r)(r.withStatementPreparator)
      }
      .pipe { r =>
        if (params.fetchSize != JdbcIO.ReadParam.BeamDefaultFetchSize) {
          // override default fetch size.
          r.withFetchSize(params.fetchSize)
        } else {
          r
        }
      }

    sc.applyTransform(params.configOverride(transform))
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("JdbcSelect is read-only")

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

object JdbcPartitionedRead {

  object PartitionColumn {

    // Supported types from JdbcUtil.PRESET_HELPERS
    def long(
      name: String,
      upperBound: Option[Long] = None,
      lowerBound: Option[Long] = None
    ): PartitionColumn[java.lang.Long] = new PartitionColumn(
      TypeDescriptors.longs(),
      name,
      upperBound.map(Long.box),
      lowerBound.map(Long.box)
    )

    def dateTime(
      name: String,
      upperBound: Option[DateTime] = None,
      lowerBound: Option[DateTime] = None
    ): PartitionColumn[DateTime] = new PartitionColumn(
      TypeDescriptor.of(classOf[DateTime]),
      name,
      upperBound,
      lowerBound
    )
  }

  case class PartitionColumn[T] private (
    typeDescriptor: TypeDescriptor[T],
    name: String,
    upperBound: Option[T],
    lowerBound: Option[T]
  )

  object ReadParam {
    val DefaultNumPartitions: Int = 200
    val DefaultDataSourceProviderFn: () => DataSource = null
    def defaultConfigOverride[S, T]: ReadWithPartitions[S, T] => ReadWithPartitions[S, T] = identity
  }

  final case class ReadParam[T, S](
    partitionColumn: JdbcPartitionedRead.PartitionColumn[S],
    rowMapper: ResultSet => T,
    numPartitions: Int = ReadParam.DefaultNumPartitions,
    dataSourceProviderFn: () => DataSource = ReadParam.DefaultDataSourceProviderFn,
    configOverride: ReadWithPartitions[T, S] => ReadWithPartitions[T, S] =
      ReadParam.defaultConfigOverride[T, S]
  )
}

final case class JdbcPartitionedRead[T: Coder, S](
  opts: JdbcConnectionOptions,
  table: String
) extends JdbcIO[T] {
  override type ReadP = JdbcPartitionedRead.ReadParam[T, S]
  override type WriteP = Nothing
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(opts, table)})"
  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] = {
    val coder = CoderMaterializer.beam(sc, Coder[T])
    val transform = BJdbcIO
      .readWithPartitions[T, S](params.partitionColumn.typeDescriptor)
      .withPartitionColumn(params.partitionColumn.name)
      .pipe(r => params.partitionColumn.lowerBound.fold(r)(r.withLowerBound))
      .pipe(r => params.partitionColumn.upperBound.fold(r)(r.withUpperBound))
      .pipe { r =>
        if (params.numPartitions != JdbcPartitionedRead.ReadParam.DefaultNumPartitions) {
          r.withNumPartitions(params.numPartitions)
        } else {
          r
        }
      }
      .withCoder(coder)
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(opts))
      .withTable(table)
      .withRowMapper(params.rowMapper(_))
      .pipe { r =>
        Option(params.dataSourceProviderFn)
          .map(fn => Functions.serializableFn[Void, DataSource](_ => fn()))
          .fold(r)(r.withDataSourceProviderFn)
      }

    sc.applyTransform(params.configOverride(transform))
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("JdbcPartitionRead is read-only")

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}

final case class JdbcWrite[T](opts: JdbcConnectionOptions, statement: String) extends JdbcIO[T] {
  override type ReadP = Nothing
  override type WriteP = JdbcIO.WriteParam[T]
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"JdbcIO(${JdbcIO.jdbcIoId(opts, statement)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("JdbcWrite is write-only")

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val transform = BJdbcIO
      .write[T]()
      .withDataSourceConfiguration(JdbcIO.dataSourceConfiguration(opts))
      .withStatement(statement)
      .withRetryConfiguration(params.retryConfiguration)
      .withRetryStrategy(params.retryStrategy.apply)
      .pipe { w =>
        Option(params.dataSourceProviderFn)
          .map(fn => Functions.serializableFn[Void, DataSource](_ => fn()))
          .fold(w)(w.withDataSourceProviderFn)
      }
      .pipe { w =>
        Option(params.preparedStatementSetter)
          .map[PreparedStatementSetter[T]](fn => fn(_, _))
          .fold(w)(w.withPreparedStatementSetter)
      }
      .pipe { w =>
        if (params.batchSize != JdbcIO.WriteParam.BeamDefaultBatchSize) {
          // override default batch size.
          w.withBatchSize(params.batchSize)
        } else {
          w
        }
      }
      .pipe(w => if (params.autoSharding) w.withAutoSharding() else w)

    data.applyInternal(params.configOverride(transform))
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}
