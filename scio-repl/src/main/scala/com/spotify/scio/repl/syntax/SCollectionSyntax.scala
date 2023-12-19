/*
 * Copyright 2023 Spotify AB
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

package com.spotify.scio.repl.syntax

import com.spotify.scio.values.SCollection

class ReplSCollectionOps[T](private val self: SCollection[T]) extends AnyVal {

  /** Convenience method to close the current [[ScioContext]] and collect elements. */
  def runAndCollect(): Iterator[T] = {
    val closedTap = self.materialize
    self.context
      .run()
      .waitUntilDone()
      .tap(closedTap)
      .value
  }
}

trait SCollectionSyntax {
  implicit def replSCollectionOps[T](sc: SCollection[T]): ReplSCollectionOps[T] =
    new ReplSCollectionOps(sc)
}
