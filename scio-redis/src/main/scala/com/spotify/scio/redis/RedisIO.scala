/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio.redis

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap, TapT}
import com.spotify.scio.redis.write.{RedisMutation, RedisWriteTransform}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.redis.{RedisConnectionConfiguration, RedisIO => BeamRedisIO}
import org.joda.time.Duration

sealed trait RedisIO[T] extends ScioIO[T] {
  final override val tapT: TapT.Aux[T, Nothing] = EmptyTapOf[T]
}

case class RedisConnectionOptions(
  host: String,
  port: Int,
  auth: Option[String] = None,
  useSSL: Boolean = false,
  timeout: Duration = Duration.standardSeconds(2)
)

object RedisConnectionOptions {

  private[redis] def toConnectionConfig(
    connectionOptions: RedisConnectionOptions
  ): RedisConnectionConfiguration = {
    var config = RedisConnectionConfiguration
      .create(connectionOptions.host, connectionOptions.port)
      .withTimeout(connectionOptions.timeout.getMillis.toInt)

    if (connectionOptions.useSSL) {
      config = config.enableSSL()
    }

    connectionOptions.auth.foreach(a => config = config.withAuth(a))

    config
  }

}

final case class RedisRead(connectionOptions: RedisConnectionOptions, keyPattern: String)
    extends RedisIO[(String, String)] {

  type ReadP = RedisRead.ReadParam
  type WriteP = Nothing

  override def testId: String =
    s"RedisReadIO(${connectionOptions.host}\t${connectionOptions.port}\t$keyPattern)"

  protected def write(data: SCollection[(String, String)], params: WriteP): Tap[Nothing] =
    throw new UnsupportedOperationException("RedisRead is read-only")

  protected def read(
    sc: ScioContext,
    params: RedisRead.ReadParam
  ): SCollection[(String, String)] = {
    val connectionConfig = RedisConnectionOptions.toConnectionConfig(connectionOptions)

    val read = BeamRedisIO
      .read()
      .withKeyPattern(keyPattern)
      .withConnectionConfiguration(connectionConfig)
      .withBatchSize(params.batchSize)
      .withOutputParallelization(params.outputParallelization)

    connectionOptions.auth.foreach(read.withAuth)

    sc.applyTransform(read).map(kv => kv.getKey -> kv.getValue)
  }

  def tap(read: RedisRead.ReadParam): Tap[Nothing] = EmptyTap
}

object RedisRead {
  object ReadParam {
    private[redis] val DefaultBatchSize: Int = 1000
    private[redis] val DefaultOutputParallelization: Boolean = true
  }

  final case class ReadParam private (
    batchSize: Int = ReadParam.DefaultBatchSize,
    outputParallelization: Boolean = ReadParam.DefaultOutputParallelization
  )
}

final case class RedisWrite[T <: RedisMutation](
  connectionOptions: RedisConnectionOptions
) extends RedisIO[T] {
  type ReadP = Nothing
  type WriteP = RedisWrite.WriteParam

  def tap(params: ReadP): Tap[Nothing] = EmptyTap

  override def testId: String =
    s"RedisWriteIO(${connectionOptions.host}\t${connectionOptions.port})"

  protected def read(sc: ScioContext, params: ReadP): SCollection[T] =
    throw new UnsupportedOperationException("RedisWrite is write-only")

  protected def write(data: SCollection[T], params: WriteP): Tap[Nothing] = {
    val connectionConfig = RedisConnectionOptions.toConnectionConfig(connectionOptions)

    val sink = new RedisWriteTransform[T](connectionConfig, params)

    data.transform_("Redis Write") { coll =>
      coll.applyInternal(sink)
    }
    EmptyTap
  }
}

object RedisWrite {
  object WriteParam {
    private[redis] val DefaultBatchSize: Int = 1000
  }

  final case class WriteParam private (batchSize: Int = WriteParam.DefaultBatchSize)
}
