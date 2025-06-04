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
import com.spotify.scio.coders.{Coder, CoderMaterializer}
import com.spotify.scio.io._
import com.spotify.scio.values.SCollection
import magnolify.neo4j.ValueType
import org.apache.beam.sdk.io.{neo4j => beam}
import org.neo4j.driver.{Record, Value, Values}

import scala.util.matching.Regex

object Neo4jIO {

  object WriteParam {
    val DefaultBatchSize: Long = 5000L
  }
  final case class WriteParam private (batchSize: Long = WriteParam.DefaultBatchSize)

  implicit private[neo4j] def recordConverter(record: Record): Value =
    Values.value(record.asMap(identity[Value]))

  private[neo4j] val UnwindParameterRegex: Regex = """UNWIND \$(\w+)""".r.unanchored

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

final case class Neo4jIO[T](neo4jOptions: Neo4jOptions, cypher: String)(implicit
  neo4jType: ValueType[T],
  coder: Coder[T]
) extends ScioIO[T] {

  import Neo4jIO._

  override type ReadP = Unit
  override type WriteP = WriteParam
  override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]

  override def testId: String = s"Neo4jIO(${neo4jIoId(neo4jOptions, cypher)})"

  override protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    sc.parallelize(Seq(()))
      .applyTransform(
        beam.Neo4jIO
          .readAll[Unit, T]()
          .withDriverConfiguration(dataSourceConfiguration(neo4jOptions.connectionOptions))
          .withSessionConfig(neo4jOptions.sessionConfig)
          .withTransactionConfig(neo4jOptions.transactionConfig)
          .withCypher(cypher)
          .withRowMapper(neo4jType.from(_))
          .withCoder(CoderMaterializer.beam(sc, coder))
      )

  private[neo4j] lazy val unwindMapName: String = cypher match {
    case UnwindParameterRegex(name) => name
    case _                          =>
      throw new IllegalArgumentException(
        s"""Expected unwind cypher with parameter but got:
           |$cypher
           |See: https://neo4j.com/docs/cypher-manual/current/clauses/unwind/#unwind-creating-nodes-from-a-list-parameter
           |""".stripMargin
      )
  }

  override protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    data.applyInternal(
      beam.Neo4jIO
        .writeUnwind()
        .withUnwindMapName(unwindMapName)
        .withDriverConfiguration(dataSourceConfiguration(neo4jOptions.connectionOptions))
        .withSessionConfig(neo4jOptions.sessionConfig)
        .withTransactionConfig(neo4jOptions.transactionConfig)
        .withBatchSize(params.batchSize)
        .withCypher(cypher)
        .withParametersFunction(neo4jType.to(_).asMap())
    )
    EmptyTap
  }

  override def tap(params: ReadP): Tap[Nothing] = EmptyTap
}
