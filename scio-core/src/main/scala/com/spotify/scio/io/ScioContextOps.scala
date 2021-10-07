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

package com.spotify.scio.io

import com.spotify.scio.ScioContext
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

final class ScioContextOps(sc: ScioContext) {

  /**
   * Applies a read IO to a multi-file input. The way how files are read depends on the
   * hintMatchesManyFiles parameter.
   *
   * If the number of files is large (e.g. tens of thousands or more), set hintMatchesManyFiles to
   * true for better performance and scalability. Note that it may decrease performance if the
   * number of files is small.
   *
   * @param singleFileIO
   *   A function creating a read IO from the provided single path parameter.
   * @param multiFileIO
   *   A function creating a read IO from the list of paths.
   * @param paths
   *   List of paths to read the data from.
   * @param hintMatchesManyFiles
   *   If false (default value) then IOs are applied individually to each input file path and joined
   *   into a single output [[SCollection]] using the [[SCollection.unionAll]]. If true then first
   *   all paths are read into an PCollection[ReadableFile] using
   *   [[org.apache.beam.sdk.io.FileIO.matchAll]]/[[org.apache.beam.sdk.io.FileIO.readMatches()]]
   *   before applying a Beam IO.
   */
  private[scio] def readFiles[R, W1, W2, T: Coder](
    singleFileIO: String => ScioIO.Aux[T, R, W1],
    multiFileIO: List[String] => ScioIO.Aux[T, R, W2]
  )(readP: R, paths: List[String], hintMatchesManyFiles: Boolean = false): SCollection[T] = {
    if (hintMatchesManyFiles && !sc.isTest) {
      sc.read(multiFileIO(paths))(readP)
    } else {
      sc.unionAll(paths.map(path => sc.read(singleFileIO(path))(readP)))
    }
  }

}
