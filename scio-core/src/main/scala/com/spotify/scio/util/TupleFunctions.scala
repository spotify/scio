/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.util

import java.lang.{Iterable => JIterable}
import java.util.{List => JList}

import org.apache.beam.sdk.values.KV

import scala.collection.JavaConverters._

private[scio] object TupleFunctions {

  def kvToTuple[K, V](kv: KV[K, V]): (K, V) = (kv.getKey, kv.getValue)

  def kvIterableToTuple[K, V](kv: KV[K, JIterable[V]]): (K, Iterable[V]) =
    (kv.getKey, kv.getValue.asScala)

  def kvListToTuple[K, V](kv: KV[K, JList[V]]): (K, Iterable[V]) =
    (kv.getKey, kv.getValue.asInstanceOf[JIterable[V]].asScala)

}
