/*
 * Copyright 2020 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.spotify.scio.extra.sparkey.instances

import com.spotify.scio.util.Cache
import com.spotify.sparkey.SparkeyReader

/** A wrapper around `SparkeyReader` that includes an in-memory Caffeine cache. */
class CachedStringSparkeyReader(val sparkey: SparkeyReader, val cache: Cache[String, String])
    extends StringSparkeyReader(sparkey) {
  override def get(key: String): Option[String] =
    Option(cache.get(key, sparkey.getAsString(key)))

  def close(): Unit = {
    sparkey.close()
    cache.invalidateAll()
  }
}
