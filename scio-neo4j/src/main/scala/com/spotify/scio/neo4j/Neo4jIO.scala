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

import scala.jdk.CollectionConverters._
import scala.util.matching.Regex

sealed trait Neo4jIO[T] extends ScioIO[T]

object Neo4jIO {

  object WriteParam {
    private[neo4j] val BeamDefaultBatchSize = 5000L
  }
  final case class WriteParam(batchSize: Long = WriteParam.BeamDefaultBatchSize)

  private[neo4j] val UnwindParameterRegex: Regex = """UNWIND \$(\w+)""".r.unanchored

  final def apply[T](opts: Neo4jOptions, cypher: String): Neo4jIO[T] =
    new Neo4jIO[T] with TestIO[T] {
      final override val tapT = EmptyTapOf[T]

      override def testId: String = s"Neo4jIO(${neo4jIoId(opts, cypher)})"
    }

  private[neo4j] def neo4jIoId(opts: Neo4jOptions, cypher: String): String =
    neo4jIoId(opts.connectionOptions, cypher)

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

case class Neo4jCypher[T: RowMapper: Coder](neo4jOptions: Neo4jOptions, cypher: String)
    extends Neo4jIO[T] {
  override type ReadP = Unit
  override type WriteP = Nothing

  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"Neo4jIO(${Neo4jIO.neo4jIoId(neo4jOptions, cypher)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.parallelize(Seq(()))
      .applyTransform(
        beam.Neo4jIO
          .readAll[Unit, T]()
          .withDriverConfiguration(Neo4jIO.dataSourceConfiguration(neo4jOptions.connectionOptions))
          .withSessionConfig(neo4jOptions.sessionConfig)
          .withTransactionConfig(neo4jOptions.transactionConfig)
          .withCypher(cypher)
          .withRowMapper(RowMapper[T].apply)
      )

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("Neo4jCypher is read-only")

  override def tap(read: ReadP): Tap[Nothing] = EmptyTap
}

final case class Neo4jWriteUnwind[T: ParametersBuilder](neo4jOptions: Neo4jOptions, cypher: String)
    extends Neo4jIO[T] {
  override type ReadP = Nothing
  override type WriteP = Neo4jIO.WriteParam
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  private[neo4j] val unwindMapName: String = cypher match {
    case Neo4jIO.UnwindParameterRegex(name) => name
    case _ =>
      throw new IllegalArgumentException(
        s"""Expected unwind cypher with parameter but got:
         |$cypher
         |See: https://neo4j.com/docs/cypher-manual/current/clauses/unwind/#unwind-creating-nodes-from-a-list-parameter
         |""".stripMargin
      )
  }

  override def testId: String = s"Neo4jIO(${Neo4jIO.neo4jIoId(neo4jOptions, cypher)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("Neo4jWrite is write-only")

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    data.applyInternal(
      beam.Neo4jIO
        .writeUnwind()
        .withUnwindMapName(unwindMapName)
        .withDriverConfiguration(Neo4jIO.dataSourceConfiguration(neo4jOptions.connectionOptions))
        .withSessionConfig(neo4jOptions.sessionConfig)
        .withTransactionConfig(neo4jOptions.transactionConfig)
        .withBatchSize(params.batchSize)
        .withCypher(cypher)
        .withParametersFunction(ParametersBuilder[T].apply(_).asJava)
    )
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] =
    EmptyTap
}
