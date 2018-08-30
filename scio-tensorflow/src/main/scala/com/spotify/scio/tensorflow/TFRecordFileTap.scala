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

package com.spotify.scio.tensorflow

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{FileStorage, Tap}
import com.spotify.scio.values.SCollection

/** Tap for Tensorflow TFRecord files. */
final case class TFRecordFileTap(path: String) extends Tap[Array[Byte]] {

  import scala.language.implicitConversions

  implicit def makeTFFileStorageFunctions(s: FileStorage): TFFileStorageFunctions =
    new TFFileStorageFunctions(s)

  override def value: Iterator[Array[Byte]] = FileStorage(path).tfRecordFile
  override def open(sc: ScioContext): SCollection[Array[Byte]] = sc.tfRecordFile(path)
}
