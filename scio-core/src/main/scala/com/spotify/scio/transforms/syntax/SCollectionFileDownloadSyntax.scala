/*
 * Copyright 2020 Spotify AB
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

package com.spotify.scio.transforms.syntax

import java.nio.file.Path
import java.net.URI

import com.spotify.scio.values.SCollection
import com.spotify.scio.coders.Coder
import com.spotify.scio.transforms.FileDownloadDoFn
import com.spotify.scio.util._
import org.apache.beam.sdk.transforms.ParDo

trait SCollectionFileDownloadSyntax {

  /**
   * Enhanced version of [[com.spotify.scio.values.SCollection SCollection]] with
   * [[java.net.URI URI]] methods.
   */
  implicit class FileDownloadSCollection(private val self: SCollection[URI]) {

    /**
     * Download [[java.net.URI URI]] elements and process as local [[java.nio.file.Path Path]] s.
     * @param batchSize
     *   batch size when downloading files
     * @param keep
     *   keep downloaded files after processing
     */
    def mapFile[T: Coder](
      f: Path => T,
      batchSize: Int = 10,
      keep: Boolean = false
    ): SCollection[T] =
      self.applyTransform(
        ParDo.of(
          new FileDownloadDoFn[T](
            RemoteFileUtil.create(self.context.options),
            Functions.serializableFn(f),
            batchSize,
            keep
          )
        )
      )

    /**
     * Download [[java.net.URI URI]] elements and process as local [[java.nio.file.Path Path]] s.
     * @param batchSize
     *   batch size when downloading files
     * @param keep
     *   keep downloaded files after processing
     */
    def flatMapFile[T: Coder](
      f: Path => TraversableOnce[T],
      batchSize: Int = 10,
      keep: Boolean = false
    ): SCollection[T] =
      self
        .applyTransform(
          ParDo.of(
            new FileDownloadDoFn[TraversableOnce[T]](
              RemoteFileUtil.create(self.context.options),
              Functions.serializableFn(f),
              batchSize,
              keep
            )
          )
        )
        .flatMap(identity)
  }
}
