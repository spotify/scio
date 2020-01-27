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

package object hash {
  implicit class ApproxFilterIterable[T](val self: Iterable[T]) extends AnyVal {
    def asApproxFilter[C <: ApproxFilterCompanion](c: C)(implicit hash: c.Hash[T]): c.Filter[T] =
      c.create(self)

    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long)(
      implicit hash: c.Hash[T]
    ): c.Filter[T] =
      c.create(self, expectedInsertions)

    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long, fpp: Double)(
      implicit hash: c.Hash[T]
    ): c.Filter[T] =
      c.create(self, expectedInsertions, fpp)
  }

  implicit class ApproxFilterSCollection[T](val self: SCollection[T]) extends AnyVal {
    def asApproxFilter[C <: ApproxFilterCompanion](
      c: C
    )(implicit hash: c.Hash[T]): SCollection[c.Filter[T]] =
      c.create(self)

    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long)(
      implicit hash: c.Hash[T]
    ): SCollection[c.Filter[T]] =
      c.create(self, expectedInsertions)

    def asApproxFilter[C <: ApproxFilterCompanion](c: C, expectedInsertions: Long, fpp: Double)(
      implicit hash: c.Hash[T]
    ): SCollection[c.Filter[T]] =
      c.create(self, expectedInsertions, fpp)
  }
}
