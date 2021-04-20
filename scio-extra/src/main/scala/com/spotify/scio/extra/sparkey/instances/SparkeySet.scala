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

import com.spotify.sparkey.SparkeyReader
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.util.CoderUtils

import scala.jdk.CollectionConverters._

/**
 * Enhanced version of `SparkeyReader` that assumes the underlying
 * Sparkey is encoded with a given Coder, but contains no values
 * (i.e.: only used as an on-disk HashSet).
 */
class SparkeySet[T](val sparkey: SparkeyReader, val coder: Coder[T]) extends SparkeySetBase[T] {

  override def contains(elem: T): Boolean =
    sparkey.getAsEntry(CoderUtils.encodeToByteArray(coder, elem)) != null

  override def iterator: Iterator[T] =
    sparkey.iterator.asScala.map(e => CoderUtils.decodeFromByteArray(coder, e.getKey))
}
