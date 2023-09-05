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

package com.spotify.scio.smb

import com.spotify.scio.coders.Coder
import com.spotify.scio.io.{KeyedIO, TapOf, TapT, TestIO}
import com.spotify.scio.util.ScioUtil

final class SortedBucketIO[K, T](path: String, override val keyBy: T => K)(implicit
  override val keyCoder: Coder[K]
) extends TestIO[T]
    with KeyedIO[T] {
  override type KeyT = K
  override val tapT: TapT.Aux[T, T] = TapOf[T]
  override def testId: String = SortedBucketIO.testId(path)
}

object SortedBucketIO {
  def apply[K: Coder, T](path: String, keyBy: T => K): SortedBucketIO[K, T] =
    new SortedBucketIO[K, T](path, keyBy)

  def testId(paths: String*): String = {
    val normalizedPaths = paths.map(p => ScioUtil.strippedPath(p) + "/").mkString(",")
    s"SortedBucketIO($normalizedPaths)"
  }
}
