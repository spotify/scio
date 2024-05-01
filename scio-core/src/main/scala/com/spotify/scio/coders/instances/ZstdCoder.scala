/*
 * Copyright 2024 Spotify AB.
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

package com.spotify.scio.coders.instances

import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.coders.{ZstdCoder => BZstdCoder}

import scala.reflect.ClassTag

object ZstdCoder {
  def apply[T: Coder: ClassTag](dict: Array[Byte]): Coder[T] =
    Coder.transform(Coder[T])(tCoder => Coder.beam(BZstdCoder.of(tCoder, dict)))

  def tuple2[K: Coder, V: Coder](
    keyDict: Array[Byte] = null,
    valueDict: Array[Byte] = null
  ): Coder[(K, V)] =
    Coder.transform(Coder[K]) { kCoder =>
      val bKCoder = Option(keyDict).map(BZstdCoder.of(kCoder, _)).getOrElse(kCoder)
      Coder.transform(Coder[V]) { vCoder =>
        val bVCoder = Option(valueDict).map(BZstdCoder.of(vCoder, _)).getOrElse(vCoder)
        Coder.beam(
          new Tuple2Coder[K, V](bKCoder, bVCoder)
        )
      }
    }
}
