/*
 * Copyright 2022 Spotify AB.
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

package com.spotify.scio.neo4j

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT, TestIO}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{neo4j => beam}
import org.neo4j.driver.Record

sealed trait Neo4jIO[T] extends ScioIO[T]

object Neo4jIO {
  final def apply[T](opts: Neo4jIoOptions): Neo4jIO[T] =
    new Neo4jIO[T] with TestIO[T] {
      final override val tapT = EmptyTapOf[T]

      override def testId: String = s"Neo4jIO(${neo4jIoId(opts)})"
    }

  private[neo4j] def neo4jIoId(opts: Neo4jIoOptions): String =
    neo4jIoId(opts.connectionOptions, opts.cypher)

  private[neo4j] def neo4jIoId(opts: Neo4jConnectionOptions, cypher: String): String =
    s"${opts.username}:${opts.password}@${opts.url}:$cypher"

  private[neo4j] def dataSourceConfiguration(
    connectionOptions: Neo4jConnectionOptions
  ): beam.Neo4jIO.DriverConfiguration =
    beam.Neo4jIO.DriverConfiguration.create(
      connectionOptions.url,
      connectionOptions.username,
      connectionOptions.password
    )
}

case class Neo4jCypher[T: Coder](readOptions: Neo4jReadOptions[T]) extends Neo4jIO[T] {
  override type ReadP = Unit
  override type WriteP = Nothing

  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"Neo4jIO(${Neo4jIO.neo4jIoId(readOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.parallelize(Seq(()))
      .applyTransform(
        beam.Neo4jIO
          .readAll[Unit, T]()
          .withDriverConfiguration(Neo4jIO.dataSourceConfiguration(readOptions.connectionOptions))
          .withCypher(readOptions.cypher)
          .withSessionConfig(readOptions.sessionConfig)
          .withTransactionConfig(readOptions.transactionConfig)
          .withRowMapper(new beam.Neo4jIO.RowMapper[T] {
            override def mapRow(record: Record): T =
              readOptions.rowMapper(record)
          })
      )

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("Neo4jCypher is read-only")

  override def tap(read: ReadP): Tap[Nothing] = EmptyTap
}

final case class Neo4jWrite[T](writeOptions: Neo4jWriteOptions[T]) extends Neo4jIO[T] {
  override type ReadP = Nothing
  override type WriteP = Unit
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"Neo4jIO(${Neo4jIO.neo4jIoId(writeOptions)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("Neo4jWrite is write-only")

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    data.applyInternal(
      beam.Neo4jIO
        .writeUnwind()
        .withUnwindMapName(writeOptions.unwindMapName)
        .withBatchSize(writeOptions.batchSize)
        .withSessionConfig(writeOptions.sessionConfig)
        .withDriverConfiguration(Neo4jIO.dataSourceConfiguration(writeOptions.connectionOptions))
        .withCypher(writeOptions.cypher)
        .withTransactionConfig(writeOptions.transactionConfig)
        .withParametersFunction((input: T) => writeOptions.parametersFunction(input))
    )
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}
