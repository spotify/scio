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

package com.spotify.scio.extra.annoy

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import annoy4s._
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.sun.jna.Native
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions
import org.apache.beam.sdk.options.PipelineOptions

/**
 * Represents the base URI for an Annoy tree, either on the local or a remote file system.
 */
trait AnnoyUri extends Serializable {
  val path: String
  private[annoy] def getReader(metric: ScioAnnoyMetric, dim: Int): AnnoyReader
  private[annoy] def saveAndClose(annoyIndex: AnnoyWriter): Unit
  private[annoy] def exists: Boolean
  override def toString: String = path
}

private[annoy] object AnnoyUri {

  def apply(path: String, opts: PipelineOptions): AnnoyUri =
    if (ScioUtil.isLocalUri(new URI(path))) {
      new LocalAnnoyUri(path)
    } else {
      new RemoteAnnoyUri(path, opts.as(classOf[GcsOptions]))
    }

}

private class LocalAnnoyUri(val path: String) extends AnnoyUri {

  override private[annoy] def getReader(metric: ScioAnnoyMetric, dim: Int): AnnoyReader
  = new AnnoyReader(path, metric, dim)
  override private[annoy] def saveAndClose(w: AnnoyWriter): Unit = {
    try {
      w.build()
      w.save(path.toString)
    } finally {
      w.free()
    }
  }
  override private[annoy] def exists: Boolean = new File(path).exists()

}

private class RemoteAnnoyUri(val path: String, options: PipelineOptions) extends AnnoyUri {

  val rfu: RemoteFileUtil = RemoteFileUtil.create(options)

  override private[annoy] def getReader(metric: ScioAnnoyMetric, dim: Int)
  : AnnoyReader = {
    val localPath = rfu.download(new URI(path))
    new AnnoyReader(localPath.toString(), metric, dim)
  }
  override private[annoy] def saveAndClose(w: AnnoyWriter): Unit = {
    val tempFile = Files.createTempDirectory("annoy").resolve("data")
    try {
      w.build()
      w.save(tempFile.toString)
    } finally {
      w.free()
    }
    rfu.upload(Paths.get(tempFile.toString), new URI(path))
    Files.delete(tempFile)
  }
  override private[annoy] def exists: Boolean = rfu.remoteExists(new URI(path))

}

private[annoy] class AnnoyWriter(metric: ScioAnnoyMetric, dim: Int, nTrees: Int) {

  val annoy4sIndex = metric match {
    case Angular => AnnoyWriter.lib.createAngular(dim)
    case Euclidean => AnnoyWriter.lib.createAngular(dim)
  }

  def addItem(item: Int, w: Array[Float]): Unit = AnnoyWriter.lib.addItem(annoy4sIndex, item, w)
  def save(filename: String): Unit = AnnoyWriter.lib.save(annoy4sIndex, filename)
  def build(): Unit = AnnoyWriter.lib.build(annoy4sIndex, nTrees)
  def free(): Unit = AnnoyWriter.lib.deleteIndex(annoy4sIndex)
  def size: Int = AnnoyWriter.lib.getNItems(annoy4sIndex)
  def verbose(b: Boolean): Unit = AnnoyWriter.lib.verbose(annoy4sIndex, b)

}

private[annoy] object AnnoyWriter {
  val lib = Native.loadLibrary("annoy", classOf[AnnoyLibrary])
    .asInstanceOf[AnnoyLibrary]
}

