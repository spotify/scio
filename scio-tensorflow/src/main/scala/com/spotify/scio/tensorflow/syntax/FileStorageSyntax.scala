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

package com.spotify.scio.tensorflow.syntax

import java.io.InputStream

import com.spotify.scio.io.FileStorage
import com.spotify.scio.tensorflow.TFRecordCodec
import org.apache.beam.sdk.io.Compression

/** File storage functions for Tensorflow TFRecord files. */
final class FileStorageOps(private val self: FileStorage) extends AnyVal {
  def tfRecordFile: Iterator[Array[Byte]] =
    new Iterator[Array[Byte]] {
      private def wrapInputStream(in: InputStream) =
        TFRecordCodec.wrapInputStream(in, Compression.AUTO)
      private val input =
        self.getDirectoryInputStream(self.path, wrapInputStream)
      private var current: Array[Byte] = TFRecordCodec.read(input)
      override def hasNext: Boolean = current != null
      override def next(): Array[Byte] = {
        val r = current
        current = TFRecordCodec.read(input)
        r
      }
    }
}

trait FileStorageSyntax {
  implicit private[scio] def tensorFlowFileStorageFunctions(s: FileStorage): FileStorageOps =
    new FileStorageOps(s)
}
