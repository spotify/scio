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

package com.spotify.scio.extra.sparkey

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import com.spotify.scio.coders.Coder
import com.spotify.sparkey.extra.ThreadLocalSparkeyReader
import com.spotify.sparkey.{Sparkey, SparkeyReader}
import org.apache.beam.sdk.options.PipelineOptions

import scala.collection.JavaConverters._

/**
 * Represents the base URI for a Sparkey index and log file, either on the local or a remote file
 * system. For remote file systems, `basePath` should be in the form
 * 'scheme://<bucket>/<path>/<sparkey-prefix>'. For local files, it should be in the form
 * '/<path>/<sparkey-prefix>'. Note that `basePath` must not be a folder or GCS bucket as it is
 * a base path representing two files - <sparkey-prefix>.spi and <sparkey-prefix>.spl.
 */
trait SparkeyUri extends Serializable {
  val basePath: String
  def getReader: SparkeyReader
  private[sparkey] def exists: Boolean
  override def toString: String = basePath
}

private[sparkey] object SparkeyUri {
  def apply(basePath: String, opts: PipelineOptions): SparkeyUri =
    if (ScioUtil.isLocalUri(new URI(basePath))) {
      new LocalSparkeyUri(basePath)
    } else {
      new RemoteSparkeyUri(basePath, opts)
    }
  def extensions: Seq[String] = Seq(".spi", ".spl")

  implicit def coderSparkeyURI: Coder[SparkeyUri] = Coder.kryo[SparkeyUri]
}

private class LocalSparkeyUri(val basePath: String) extends SparkeyUri {
  override def getReader: SparkeyReader = new ThreadLocalSparkeyReader(new File(basePath))
  override private[sparkey] def exists: Boolean = {
    SparkeyUri.extensions.map(e => new File(basePath + e)).exists(_.exists)
  }
}

private class RemoteSparkeyUri(val basePath: String, options: PipelineOptions) extends SparkeyUri {

  val rfu: RemoteFileUtil = RemoteFileUtil.create(options)

  override def getReader: SparkeyReader = {
    val uris = SparkeyUri.extensions.map(e => new URI(basePath + e))
    val paths = rfu.download(uris.asJava).asScala
    new ThreadLocalSparkeyReader(paths.head.toFile)
  }
  override private[sparkey] def exists: Boolean =
    SparkeyUri.extensions
      .exists(e => rfu.remoteExists(new URI(basePath + e)))
}

private[sparkey] class SparkeyWriter(val uri: SparkeyUri, maxMemoryUsage: Long = -1) {

  private val localFile = uri match {
    case u: LocalSparkeyUri => u.basePath
    case _: RemoteSparkeyUri => Files.createTempDirectory("sparkey-").resolve("data").toString
  }

  private lazy val delegate = Sparkey.createNew(new File(localFile))

  def put(key: String, value: String): Unit = delegate.put(key, value)

  def put(key: Array[Byte], value: Array[Byte]): Unit = delegate.put(key, value)

  def close(): Unit = {
    delegate.flush()
    if (maxMemoryUsage > 0) {
      delegate.setMaxMemory(maxMemoryUsage)
    }
    delegate.writeHash()
    delegate.close()
    uri match {
      case u: RemoteSparkeyUri =>
        // Copy .spi and .spl to GCS
        SparkeyUri.extensions.foreach { e =>
          val src = Paths.get(localFile + e)
          val dst = new URI(u.basePath + e)
          u.rfu.upload(src, dst)
        }
      case _ => ()
    }
  }

}
