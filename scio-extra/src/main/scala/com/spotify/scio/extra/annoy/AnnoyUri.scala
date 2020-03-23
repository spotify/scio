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

package com.spotify.scio.extra.annoy

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import annoy4s._
import com.spotify.scio.coders.Coder
import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import org.apache.beam.sdk.options.PipelineOptions

/**
 * Represents the base URI for an Annoy tree, either on the local or a remote file system.
 */
trait AnnoyUri extends Serializable {
  val path: String
  private[annoy] def getReader(metric: AnnoyMetric, dim: Int): AnnoyReader
  private[annoy] def saveAndClose(annoyIndex: AnnoyWriter): Unit
  private[annoy] def exists: Boolean
  override def toString: String = path
}

private[annoy] object AnnoyUri {
  def apply(path: String, opts: PipelineOptions): AnnoyUri =
    if (ScioUtil.isLocalUri(new URI(path))) {
      new LocalAnnoyUri(path)
    } else {
      new RemoteAnnoyUri(path, opts)
    }

  implicit val annoyUriCoder: Coder[AnnoyUri] = Coder.kryo[AnnoyUri]
}

private class LocalAnnoyUri(val path: String) extends AnnoyUri {
  override private[annoy] def getReader(metric: AnnoyMetric, dim: Int): AnnoyReader =
    new AnnoyReader(path, metric, dim)
  override private[annoy] def saveAndClose(w: AnnoyWriter): Unit =
    try {
      w.build()
      w.save(path.toString)
    } finally {
      w.free()
    }
  override private[annoy] def exists: Boolean = new File(path).exists()
}

private class RemoteAnnoyUri(val path: String, options: PipelineOptions) extends AnnoyUri {
  private[this] val rfu: RemoteFileUtil = RemoteFileUtil.create(options)

  override private[annoy] def getReader(metric: AnnoyMetric, dim: Int): AnnoyReader = {
    val localPath = rfu.download(new URI(path))
    new AnnoyReader(localPath.toString, metric, dim)
  }
  override private[annoy] def saveAndClose(w: AnnoyWriter): Unit = {
    val tempFile = Files.createTempDirectory("annoy-").resolve("data")
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

private[annoy] class AnnoyWriter(metric: AnnoyMetric, dim: Int, nTrees: Int) {
  private[this] val annoy4sIndex = metric match {
    case com.spotify.scio.extra.annoy.Angular   => Annoy.annoyLib.createAngular(dim)
    case com.spotify.scio.extra.annoy.Euclidean => Annoy.annoyLib.createEuclidean(dim)
  }

  def addItem(item: Int, w: Array[Float]): Unit = {
    Annoy.annoyLib.addItem(annoy4sIndex, item, w)
    ()
  }
  def save(filename: String): Unit = {
    Annoy.annoyLib.save(annoy4sIndex, filename)
    ()
  }
  def build(): Unit = Annoy.annoyLib.build(annoy4sIndex, nTrees)
  def free(): Unit = Annoy.annoyLib.deleteIndex(annoy4sIndex)
  def size: Int = Annoy.annoyLib.getNItems(annoy4sIndex)
  def verbose(b: Boolean): Unit = Annoy.annoyLib.verbose(annoy4sIndex, b)
}
