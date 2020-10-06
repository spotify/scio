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

import com.spotify.scio.ScioContext
import com.spotify.scio.redis.RedisRead.ReadParam
import com.spotify.scio.values.SCollection
import com.spotify.scio.redis.{RedisConnectionOptions, RedisRead}

final class ScioContextSyntax(private val sc: ScioContext) extends AnyVal {

  def redis(host: String,
            port: Int,
            keyPattern: String,
            auth: Option[String] = ReadParam.DefaultAuth,
            useSsl: Boolean = ReadParam.DefaultUseSsl,
            batchSize: Int = ReadParam.DefaultBatchSize,
            timeout: Int = ReadParam.DefaultTimeout,
            outputParallelization: Boolean = ReadParam.DefaultOutputParallelization)
  : SCollection[(String, String)] = {
    val connectionOptions = RedisConnectionOptions(host, port, auth, useSsl)
    val params = ReadParam(batchSize, timeout, outputParallelization)
    sc.read(RedisRead(connectionOptions, keyPattern))(params)
  }

}
