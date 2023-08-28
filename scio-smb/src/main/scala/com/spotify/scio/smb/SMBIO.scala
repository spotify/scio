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
import org.apache.beam.sdk.extensions.smb.SortedBucketIO

import scala.jdk.CollectionConverters._

final case class SMBIO[K: Coder, T](id: String, keyBy: T => K) extends TestIO[T] with KeyedIO[T] {
  override type KeyT = K
  override val tapT: TapT.Aux[T, T] = TapOf[T]
  override def testId: String = SMBIO.testId(id)
  override def keyCoder: Coder[K] = Coder[K]
}

object SMBIO {
  private[smb] def testId(read: SortedBucketIO.Read[_]): String =
    testId(read.getInputDirectories.asScala.mkString(","))
  private def testId(id: String): String = s"SMBIO($id)"
}
