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

package com.spotify.scio.redis.syntax

import com.spotify.scio.io.ClosedTap
import com.spotify.scio.redis.{RedisConnectionOptions, RedisWrite}
import com.spotify.scio.redis.RedisWrite.WriteParam
import com.spotify.scio.redis.write.{RedisMutation, RedisMutator}
import com.spotify.scio.values.SCollection

final class SCollectionRedisOps[T <: RedisMutation[_] : RedisMutator]
(private val self: SCollection[T]) {

  def saveAsRedis(
    connectionOptions: RedisConnectionOptions,
    batchSize: Int = RedisWrite.WriteParam.DefaultBatchSize
  ): ClosedTap[Nothing] = {
    val params = WriteParam(batchSize)
    self.write(RedisWrite[T](connectionOptions))(params)
  }

}

trait SCollectionSyntax {
  implicit def redisSCollectionOps[T <: RedisMutation[_] : RedisMutator](
    coll: SCollection[T]
  ): SCollectionRedisOps[T] = new SCollectionRedisOps[T](coll)
}
