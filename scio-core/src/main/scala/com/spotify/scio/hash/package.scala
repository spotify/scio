/*
 * Copyright 2020 Spotify AB.
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

package com.spotify.scio

import com.spotify.scio.values.SCollection

/**
 * Main package for hash APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.hash._
 * }}}
 */
package object hash {
  implicit class ApproxFilterIterable[T](private val self: Iterable[T]) extends AnyVal {

    /**
     * Creates an [[ApproxFilter]] from this [[Iterable]] with the collection size as
     * `expectedInsertions` and default `fpp` of 0.03.
     *
     * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * @param c companion object of the [[ApproxFilter]] implementation, e.g. [[BloomFilter]].
     */
    def asApproxFilter[C <: ApproxFilterCompanion](c: C)(implicit hash: c.Hash[T]): c.Filter[T] =
      c.create(self)

    /**
     * Creates an [[ApproxFilter]] from this [[Iterable]] with the expected number of insertions
     * and default `fpp` of 0.03.
     *
     * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * @param c companion object of the [[ApproxFilter]] implementation, e.g. [[BloomFilter]].
     */
    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long)(
      implicit hash: c.Hash[T]
    ): c.Filter[T] =
      c.create(self, expectedInsertions)

    /**
     * Creates an [[ApproxFilter]] from this [[Iterable]] with the expected number of insertions
     * and expected false positive probability.
     *
     * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * @param c companion object of the [[ApproxFilter]] implementation, e.g. [[BloomFilter]].
     */
    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long, fpp: Double)(
      implicit hash: c.Hash[T]
    ): c.Filter[T] =
      c.create(self, expectedInsertions, fpp)
  }

  implicit class ApproxFilterSCollection[T](private val self: SCollection[T]) extends AnyVal {

    /**
     * Creates an [[ApproxFilter]] from this [[SCollection]] with the collection size as
     * `expectedInsertions` and default `fpp` of 0.03.
     *
     * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * @param c companion object of the [[ApproxFilter]] implementation, e.g. [[BloomFilter]].
     */
    def asApproxFilter[C <: ApproxFilterCompanion](
      c: C
    )(implicit hash: c.Hash[T]): SCollection[c.Filter[T]] =
      c.create(self)

    /**
     * Creates an [[ApproxFilter]] from this [[SCollection]] with the expected number of insertions
     * and default `fpp` of 0.03.
     *
     * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * @param c companion object of the [[ApproxFilter]] implementation, e.g. [[BloomFilter]].
     */
    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long)(
      implicit hash: c.Hash[T]
    ): SCollection[c.Filter[T]] =
      c.create(self, expectedInsertions)

    /**
     * Creates an [[ApproxFilter]] from this [[SCollection]] with the expected number of insertions
     * and expected false positive probability.
     *
     * Note that overflowing an [[ApproxFilter]] with significantly more elements than specified,
     * will result in its saturation, and a sharp deterioration of its false positive probability.
     *
     * @param c companion object of the [[ApproxFilter]] implementation, e.g. [[BloomFilter]].
     */
    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long, fpp: Double)(
      implicit hash: c.Hash[T]
    ): SCollection[c.Filter[T]] =
      c.create(self, expectedInsertions, fpp)
  }
}
