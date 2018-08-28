/*
 * Copyright 2018 Spotify AB.
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

package com.twitter.algebird

// FIXME: remove this once upstream is released
// https://github.com/twitter/algebird/pull/662
class SetMonoid[T] extends Monoid[Set[T]] {
  override def zero: Set[T] = Set[T]()
  override def plus(left: Set[T], right: Set[T]): Set[T] =
    if (left.size > right.size) {
      left ++ right
    } else {
      right ++ left
    }
  override def sumOption(items: TraversableOnce[Set[T]]): Option[Set[T]] =
    if (items.isEmpty) {
      None
    } else {
      val builder = Set.newBuilder[T]
      items.foreach { s =>
        builder ++= s
      }
      Some(builder.result())
    }
}
