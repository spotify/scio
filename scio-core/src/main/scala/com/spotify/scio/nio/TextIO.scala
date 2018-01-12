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
import com.spotify.scio.testing.TestIO
import com.spotify.scio.util.ScioUtil
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.{Compression, FileBasedSink}
import org.apache.beam.sdk.{io => bio}

import scala.concurrent.Future
import scala.language.implicitConversions

case class TextIO(path: String) extends TestIO[String](path) with Tap[String] {

  /** Read data set into memory. */
  def value: Iterator[String] = FileStorage(path).textFile

  /** Open data set as an [[com.spotify.scio.values.SCollection SCollection]]. */
  def open(sc: ScioContext): SCollection[String] = sc.textFile(path)
}

object TextIO extends ScioIO[String] {
  /** Implicitly convert ScioContext to have all TextIO read helper functions. */
  implicit def read(sc: ScioContext): TextScioContext = new TextScioContext(sc)

  /**
   * Implicitly convert [[com.spotify.scio.values.SCollection SCollection[String]]] to
   * have all TextIO write helper functions
   */
  implicit def write(scol: SCollection[String]): TextSCollection = new TextSCollection(scol)

  private[scio] def pathWithShards(path: String) = path.replaceAll("\\/+$", "") + "/part"

  class TextScioContext(@transient val self: ScioContext) extends Serializable {

    /**
     * Get an SCollection for a text file.
     *
     * @group input
     */
    def textFile(path: String,
                 compression: Compression = Compression.AUTO)
    : SCollection[String] = self.requireNotClosed {
      if (self.isTest) {
        self.getTestInput(TextIO(path))
      } else {
        self.wrap(self.applyInternal(bio.TextIO.read().from(path)
          .withCompression(compression))).setName(path)
      }
    }
  }

  class TextSCollection(@transient val self: SCollection[String]) extends Serializable {

    /**
     * Save this SCollection as a text file. Note that elements must be of type `String`.
     *
     * @group output
     */
    def saveAsTextFile(path: String,
                       suffix: String = ".txt",
                       numShards: Int = 0,
                       compression: Compression = Compression.UNCOMPRESSED)
    : Future[Tap[String]] = {
      if (self.context.isTest) {
        self.context.testOut(TextIO(path))(self)
        self.saveAsInMemoryTap
      } else {
        self.applyInternal(textOut(path, suffix, numShards, compression))
        self.context.makeFuture(TextIO(ScioUtil.addPartSuffix(path)))
      }
    }

    private[scio] def textOut(path: String,
                              suffix: String,
                              numShards: Int,
                              compression: Compression) = {
      bio.TextIO.write()
        .to(pathWithShards(path))
        .withSuffix(suffix)
        .withNumShards(numShards)
        .withWritableByteChannelFactory(FileBasedSink.CompressionType.fromCanonical(compression))
    }
  }

}
