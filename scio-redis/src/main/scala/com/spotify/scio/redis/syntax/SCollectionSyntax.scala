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
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.redis.RedisIO
import org.joda.time.Duration

final class SCollectionRedisOps(private val self: SCollection[(String, String)]) {

  def saveAsRedis(
    connectionOptions: RedisConnectionOptions,
    writeMethod: RedisIO.Write.Method,
    expireTimeMillis: Option[Duration] = None
  ): ClosedTap[Nothing] = {
    val params = WriteParam(expireTimeMillis)
    self.write(RedisWrite(connectionOptions, writeMethod))(params)
  }

}

trait SCollectionSyntax {
  implicit def redisSCollectionOps(coll: SCollection[(String, String)]): SCollectionRedisOps =
    new SCollectionRedisOps(coll)
}
