/*
 * Copyright 2019 Spotify AB.
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

package com.spotify.scio.extra.sparkey.instances

import java.nio.charset.Charset

import com.spotify.scio.util.Cache
import com.spotify.sparkey.SparkeyReader

import scala.jdk.CollectionConverters._

/**
 * A wrapper around `SparkeyReader` that includes both a decoder (to map from each byte array
 * to a JVM type) and an optional in-memory cache.
 */
class TypedSparkeyReader[T](
  val sparkey: SparkeyReader,
  val decoder: Array[Byte] => T,
  val cache: Cache[String, T] = Cache.noOp
) extends SparkeyMapBase[String, T] {
  private def stringKeyToBytes(key: String): Array[Byte] = key.getBytes(Charset.defaultCharset())

  private def loadValueFromSparkey(key: String): T = {
    val value = sparkey.getAsByteArray(stringKeyToBytes(key))
    if (value == null) {
      null.asInstanceOf[T]
    } else {
      decoder(value)
    }
  }

  override def get(key: String): Option[T] =
    Option(cache.get(key, loadValueFromSparkey(key)))

  override def iterator: Iterator[(String, T)] =
    sparkey.iterator.asScala.map { e =>
      val key = e.getKeyAsString
      val value = cache.get(key).getOrElse(decoder(e.getValue))
      (key, value)
    }

  def close(): Unit = {
    sparkey.close()
    cache.invalidateAll()
  }
}
