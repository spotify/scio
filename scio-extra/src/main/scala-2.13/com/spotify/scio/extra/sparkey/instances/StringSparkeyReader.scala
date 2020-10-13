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

import com.spotify.sparkey.SparkeyReader

import scala.jdk.CollectionConverters._

/** Enhanced version of `SparkeyReader` that mimics a `Map`. */
class StringSparkeyReader(self: SparkeyReader) extends Map[String, String] {
  override def get(key: String): Option[String] =
    Option(self.getAsString(key))
  override def iterator: Iterator[(String, String)] =
    self.iterator.asScala.map(e => (e.getKeyAsString, e.getValueAsString))

  override def updated[B1 >: String](key: String, value: B1): Map[String, B1] =
    throw new NotImplementedError("Sparkey-backed map; operation not supported.")
  override def removed(key: String): Map[String, String] =
    throw new NotImplementedError("Sparkey-backed map; operation not supported.")
}
