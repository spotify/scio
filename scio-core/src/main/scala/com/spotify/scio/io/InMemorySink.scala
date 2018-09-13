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

package com.spotify.scio.io

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder

import scala.collection.concurrent.TrieMap

private[scio] object InMemorySink {

  private val cache: TrieMap[String, Iterable[Any]] = TrieMap.empty

  def save[T: Coder](id: String, data: SCollection[T]): Unit = {
    require(data.context.isTest, "In memory sink can only be used in tests")
    data
      .groupBy(_ => ())
      .values
      .map { values =>
        cache += (id -> values)
        ()
      }
  }

  def get[T](id: String): Iterable[T] = cache(id).asInstanceOf[Iterable[T]]

}
