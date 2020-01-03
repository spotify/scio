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
package com.spotify.scio.values

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

@deprecated("Use SCollection[(K, V)]#asMultiMapSideInput instead", "0.8.0")
case class SideMap[K, V](side: SideInput[MMap[K, ArrayBuffer[V]]]) {
  private[values] def asImmutableSideInput: SideInput[Map[K, Iterable[V]]] =
    side.map(_.toMap)
}
