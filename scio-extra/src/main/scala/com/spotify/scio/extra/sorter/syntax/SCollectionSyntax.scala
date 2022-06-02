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

package com.spotify.scio.extra.sorter.syntax

import com.spotify.scio.annotations.experimental
import com.spotify.scio.coders.Coder
import com.spotify.scio.extra.sorter.SortingKey
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.extensions.sorter.ExternalSorter.Options.SorterType
import org.apache.beam.sdk.extensions.sorter.{BufferedExternalSorter, SortValues}
import org.apache.beam.sdk.values.KV

import scala.collection.AbstractIterator
import scala.jdk.CollectionConverters._

final class SorterOps[K1, K2: SortingKey, V](self: SCollection[(K1, Iterable[(K2, V)])]) {

  /**
   * Takes an [[SCollection]] with elements consisting of a primary key and iterables over
   * (secondary key, value) pairs, and returns an [[SCollection]] of the same elements but with
   * values sorted lexicographicly by the secondary key.
   *
   * The secondary key needs to be encoded as a [[String]] or [[Array[Byte]]. [[SortValues]]
   * compares bytes lexicographically and may write secondary key-value pairs to disk.
   *
   * @note
   *   The primary key is explicit here only because this transform is typically used on a result of
   *   a [[PairSCollectionFunctions.groupByKey]].
   *
   * @param memoryMB
   *   Sets the size of the memory buffer in megabytes. This controls both the buffer for initial in
   *   memory sorting and the buffer used when external sorting. Must be greater than zero and less
   *   than 2048.
   */
  @experimental
  def sortValues(memoryMB: Int)(implicit
    k1Coder: Coder[K1],
    k2Coder: Coder[K2],
    vCoder: Coder[V]
  ): SCollection[(K1, Iterable[(K2, V)])] = self.transform { c =>
    val options = BufferedExternalSorter
      .options()
      .withExternalSorterType(SorterType.NATIVE)
      .withMemoryMB(memoryMB)
    // Coder implicit expansion fails. Create coder manually
    val coder = Coder.kv(k1Coder, Coder.jIterableCoder(Coder.kv(k2Coder, vCoder)))
    c.withName("TupleToKv")
      .map { case (k1, vs) => KV.of(k1, vs.map { case (k2, v) => KV.of(k2, v) }.asJava) }(coder)
      .withName("SortValues")
      .applyTransform(SortValues.create[K1, K2, V](options))(coder)
      .withName("KvToTuple")
      .map { kv =>
        val iter = new Iterable[(K2, V)] {
          override def iterator: Iterator[(K2, V)] = new AbstractIterator[(K2, V)] {
            private[this] val iter = kv.getValue.iterator()
            override def hasNext: Boolean = iter.hasNext

            override def next(): (K2, V) = {
              val next = iter.next()
              (next.getKey, next.getValue)
            }
          }

          override def toString: String = "<iterable>"
        }
        (kv.getKey, iter)
      }(Coder.beam(c.internal.getCoder))
  }
}

trait SCollectionSyntax {
  implicit def sorterOps[K1, K2: SortingKey, V](
    coll: SCollection[(K1, Iterable[(K2, V)])]
  ): SorterOps[K1, K2, V] = new SorterOps(coll)
}
