/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.nio

import com.spotify.scio.ScioContext
import com.spotify.scio.io.{FileStorage, Tap}
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{Compression, FileBasedSink, TextIO => BTextIO}

import scala.concurrent.Future

case class TextIO(path: String) extends ScioIO[String] with Tap[String] {

  case class ReadParams(compression: Compression = Compression.AUTO)

  case class WriteParams(suffix: String = ".txt",
                         numShards: Int = 0,
                         compression: Compression = Compression.UNCOMPRESSED)

  type ReadP = ReadParams
  type WriteP = WriteParams

  def read(sc: ScioContext, params: ReadParams): SCollection[String] = sc.requireNotClosed {
    if (sc.isTest) {
      // TODO: support test
      throw new UnsupportedOperationException("TextIO test is not yet supported")
    } else {
      sc.wrap(sc.applyInternal(BTextIO.read().from(path)
        .withCompression(params.compression))).setName(path)
    }
  }

  def write(pipeline: SCollection[String], params: WriteParams): Future[Tap[String]] = {
    if (pipeline.context.isTest) {
      // TODO: support test
      throw new UnsupportedOperationException("TextIO test is not yet supported")
    } else {
      pipeline.applyInternal(textOut(path, params))
      pipeline.context.makeFuture(TextIO(ScioUtil.addPartSuffix(path)))
    }
  }

  /** Read data set into memory. */
  def value: Iterator[String] = FileStorage(path).textFile

  /** Open data set as an [[com.spotify.scio.values.SCollection SCollection]]. */
  def open(sc: ScioContext): SCollection[String] = read(sc, ReadParams())

  private[scio] def textOut(path: String,
                            params: WriteParams) = {
    BTextIO.write()
      .to(pathWithShards(path))
      .withSuffix(params.suffix)
      .withNumShards(params.numShards)
      .withWritableByteChannelFactory(
        FileBasedSink.CompressionType.fromCanonical(params.compression))
  }

  private[scio] def pathWithShards(path: String) = path.replaceAll("\\/+$", "") + "/part"

}

