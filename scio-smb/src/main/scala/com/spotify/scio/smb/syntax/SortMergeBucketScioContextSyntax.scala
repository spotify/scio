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

package com.spotify.scio.smb.syntax

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values._
import org.apache.beam.sdk.extensions.smb.SortedBucketIO

import scala.collection.JavaConverters._

trait SortMergeBucketScioContextSyntax {
  implicit def asSMBScioContext(sc: ScioContext): SortedBucketScioContext =
    new SortedBucketScioContext(sc)
}

final class SortedBucketScioContext(@transient private val self: ScioContext) {
  def sortMergeJoin[K: Coder, L: Coder, R: Coder](
    keyClass: Class[K],
    lhs: SortedBucketIO.Read[L],
    rhs: SortedBucketIO.Read[R]
  ): SCollection[(K, (L, R))] =
    sortMergeCoGroup(keyClass, lhs, rhs)
      .flatMap {
        case (k, (l, r)) =>
          for {
            i <- l
            j <- r
          } yield (k, (i, j))
      }

  // @Todo: expand signatures in the style of multijoin.py
  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B]
  ): SCollection[(K, (Iterable[A], Iterable[B]))] = {
    val t = SortedBucketIO.read(keyClass).of(a).and(b)
    val (tupleTagA, tupleTagB) = (
      a.getTupleTag,
      b.getTupleTag
    )
    self
      .wrap(self.applyInternal(t))
      .map { kv =>
        val cgbkResult = kv.getValue

        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala
          )
        )
      }
  }

  def sortMergeCoGroup[K: Coder, A: Coder, B: Coder, C: Coder](
    keyClass: Class[K],
    a: SortedBucketIO.Read[A],
    b: SortedBucketIO.Read[B],
    c: SortedBucketIO.Read[C]
  ): SCollection[(K, (Iterable[A], Iterable[B], Iterable[C]))] = {
    val t = SortedBucketIO.read(keyClass).of(a).and(b).and(c)
    val (tupleTagA, tupleTagB, tupleTagC) = (
      a.getTupleTag,
      b.getTupleTag,
      c.getTupleTag
    )
    self
      .wrap(self.applyInternal(t))
      .map { kv =>
        val cgbkResult = kv.getValue

        (
          kv.getKey,
          (
            cgbkResult.getAll(tupleTagA).asScala,
            cgbkResult.getAll(tupleTagB).asScala,
            cgbkResult.getAll(tupleTagC).asScala
          )
        )
      }
  }
}
