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

package com.spotify.scio.redis.lookup

import com.spotify.scio.redis.RedisConnectionOptions
import com.spotify.scio.transforms.GuavaAsyncLookupDoFn
import redis.clients.jedis.Jedis

/**
 * A [[org.apache.beam.sdk.transforms.DoFn]] that performs asynchronous Redis lookup.
 *
 * @tparam I Type of the input element.
 * @tparam O Type of the lookup result.
 */
abstract class RedisLookupDoFn[I, O](
  connectionOptions: RedisConnectionOptions,
  maxPendingRequests: Int = RedisLookupDoFn.DEFAULT_MAX_PENDING_REQUEST
) extends GuavaAsyncLookupDoFn[I, O, ThreadLocal[Jedis]](maxPendingRequests) {

  override protected def newClient(): ThreadLocal[Jedis] = {
    val connectionConfig = RedisConnectionOptions.toConnectionConfig(connectionOptions)

    new ThreadLocal[Jedis] {
      override def initialValue(): Jedis = connectionConfig.connect()
    }
  }

}

object RedisLookupDoFn {

  val DEFAULT_MAX_PENDING_REQUEST = 1000

}
