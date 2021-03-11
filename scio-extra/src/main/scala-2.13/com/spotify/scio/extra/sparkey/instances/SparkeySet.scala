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
 * Enhanced version of `SparkeyReader` that assumes the underlying
 * Sparkey is encoded with a given Coder, but contains no values
 * (i.e.: only used as an on-disk HashSet).
 */
class SparkeySet[K](val sparkey: SparkeyReader, val koder: sdk.coders.Coder[K]) extends Set[K] {

  override def incl(elem: K): Set[K] =
    throw new NotImplementedError("Sparkey-backed set; operation not supported.")

  override def excl(elem: K): Set[K] =
    throw new NotImplementedError("Sparkey-backed set; operation not supported.")

  // getAsEntry is used here on purpose to avoid hitting disk just to find an empty value there.
  override def contains(key: K): Boolean = sparkey.getAsEntry(encode(key, koder)) != null

  override def iterator: Iterator[K] = sparkey.iterator.asScala.map(e => decode(e.getKey, koder))
}
