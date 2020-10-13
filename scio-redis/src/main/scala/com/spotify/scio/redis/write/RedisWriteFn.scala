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

package com.spotify.scio.redis.write

import com.spotify.scio.redis.RedisWrite
import org.apache.beam.sdk.io.redis.RedisConnectionConfiguration
import org.apache.beam.sdk.transforms.DoFn._
import org.apache.beam.sdk.transforms.{DoFn, PTransform, ParDo}
import org.apache.beam.sdk.values.{PCollection, PDone}
import redis.clients.jedis.{Jedis, Pipeline}

final class RedisWriteFn[T <: RedisMutation[_] : RedisMutator](
  connectionConfig: RedisConnectionConfiguration,
  writeParams: RedisWrite.WriteParam
) extends DoFn[T, Void] {

  @transient private var jedis: Jedis = _
  @transient private var pipeline: Pipeline = _

  private var batchCount = 0

  @Setup
  def setup(): Unit =
    jedis = connectionConfig.connect

  @StartBundle
  def startBundle(): Unit = {
    pipeline = jedis.pipelined
    pipeline.multi
    batchCount = 0
  }

  @ProcessElement
  def processElement(c: ProcessContext): Unit = {
    RedisMutator.mutate(pipeline)(c.element())

    batchCount += 1
    if (batchCount >= writeParams.batchSize) {
      pipeline.exec
      pipeline.sync()
      pipeline.multi
      batchCount = 0
    }
  }

  @FinishBundle
  def finishBundle(): Unit = {
    if (pipeline.isInMulti) {
      pipeline.exec
      pipeline.sync()
    }
    batchCount = 0
  }

  @Teardown def teardown(): Unit =
    jedis.close()

}

final class RedisWriteTransform[T <: RedisMutation[_] : RedisMutator](
  connectionConfig: RedisConnectionConfiguration,
  writeParams: RedisWrite.WriteParam
) extends PTransform[PCollection[T], PDone] {

  override def expand(input: PCollection[T]): PDone = {
    input.apply(ParDo.of(new RedisWriteFn(connectionConfig, writeParams)))

    PDone.in(input.getPipeline)
  }

}
