/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.scio.extra.sparkey

import com.spotify.scio.extra.sparkey.instances.SparkeySet
import com.spotify.scio.values.{SCollection, SideInput}

/** Extra functions available on SCollections for Sparkey hash-based filtering. */
class LargeHashSCollectionFunctions[T](private val self: SCollection[T]) {

  /**
   * Return a new SCollection containing only elements that also exist in the `LargeSetSideInput`.
   *
   * @group transform
   */
  def hashFilter(sideInput: SideInput[SparkeySet[T]]): SCollection[T] = {
    implicit val coder = self.coder
    self.map((_, ())).hashIntersectByKey(sideInput).keys
  }
}
