/*
 * Copyright 2017 Spotify AB.
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

package com.spotify.scio.io

import java.io.InputStream


/** File storage functions for Tensorflow TFRecord files. */
class TFFileStorageFunctions(self: FileStorage) {

  def tfRecordFile: Iterator[Array[Byte]] = {
    new Iterator[Array[Byte]] {
      private def wrapInputStream(in: InputStream) =
        TFRecordCodec.wrapInputStream(in, TFRecordOptions.readDefault)
      private val input = self.getDirectoryInputStream(self.path, wrapInputStream)
      private var current: Array[Byte] = TFRecordCodec.read(input)
      override def hasNext: Boolean = current != null
      override def next(): Array[Byte] = {
        val r = current
        current = TFRecordCodec.read(input)
        r
      }
    }
  }

}
