/*
 * Copyright 2021 Spotify AB
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

import com.spotify.scio.extra.sparkey.instances.SparkeyCoderUtils.{decode, encode}
import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk

import scala.jdk.CollectionConverters._

/**
 * Enhanced version of [[SparkeyReader]] that assumes the underlying Sparkey is encoded with the
 * given Coders, providing a very similar interface to Map[K, V].
 */
class SparkeyMap[K, V](
  val sparkey: SparkeyReader,
  val koder: sdk.coders.Coder[K],
  val voder: sdk.coders.Coder[V]
) extends SparkeyMapBase[K, V] {

  private def loadValueFromSparkey(key: K): V = {
    val value = sparkey.getAsByteArray(encode(key, koder))
    if (value == null) {
      null.asInstanceOf[V]
    } else {
      decode(value, voder)
    }
  }

  override def get(key: K): Option[V] = Option(loadValueFromSparkey(key))

  override def iterator: Iterator[(K, V)] =
    sparkey.iterator.asScala.map { e =>
      val key = decode(e.getKey, koder)
      val value = decode(e.getValue, voder)
      (key, value)
    }

  def close(): Unit = sparkey.close()

  override def contains(key: K): Boolean = sparkey.getAsEntry(encode(key, koder)) != null
}
