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
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.{io => beam}

final class ScioContextOps(sc: ScioContext) {

  /**
   * Applies a read IO to a multi-file input. The way how files are read depends on the
   * hintMatchesManyFiles parameter.
   *
   * If the number of files is large (e.g. tens of thousands or more), set hintMatchesManyFiles
   * to true for better performance and scalability. Note that it may decrease performance if the
   * number of files is small.
   *
   * @param singleFileTransform Function for reading a single file into an SCollection.
   * @param multiFileTransform Beam's PTransform with ReadableFile as an input.
   * @param paths List of paths to read the data from.
   * @param hintMatchesManyFiles
   * If false (default value) then IOs are applied individually to each input file path and
   * joined into a single output [[SCollection]] using the [[SCollection.unionAll]].
   * If true then first all paths are read into an PCollection[ReadableFile] using
   * [[org.apache.beam.sdk.io.FileIO.matchAll]]/[[org.apache.beam.sdk.io.FileIO.matchAll]] before
   * applying a Beam IO.
   */
  private[scio] def readFiles[T : Coder](singleFileTransform: String => SCollection[T],
                                         multiFileTransform: MultiFilePTransform[T])
                                        (paths: List[String], hintMatchesManyFiles: Boolean = false)
  : SCollection[T] = {
    val coll = if(hintMatchesManyFiles && !sc.isTest) {
      sc.parallelize(paths).readFiles(multiFileTransform)
    } else {
      sc.unionAll(paths.map(singleFileTransform))
    }

    coll.withName("Multiple Files Read")
  }

}
