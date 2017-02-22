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
import java.nio.ByteBuffer
import java.nio.file.{Files, Paths}

import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.util.ScioUtil
import com.spotify.sparkey.{CompressionType, Sparkey, SparkeyReader}
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.util.GcsUtil.GcsUtilFactory
import org.apache.beam.sdk.util.gcsfs.GcsPath

/**
 * Represents the base URI for a Sparkey index and log file, either on the local file
 * system or on GCS. For GCS, the URI should be in the form
 * 'gs://<bucket>/<path>/<sparkey-prefix>'. For local files, the URI should be in the form
 * '/<path>/<sparkey-prefix>'. Note that the URI must not be a folder or GCS bucket as the URI is
 * a base path representing two files - <sparkey-prefix>.spi and <sparkey-prefix>.spl.
 */
trait SparkeyUri {
  val basePath: String
  def getReader(): SparkeyReader
  override def toString(): String = basePath
}

object SparkeyUri {
  def apply(path: String): SparkeyUri =
    if (ScioUtil.isGcsUri(new URI(path))) new GcsSparkeyUri(path) else new LocalSparkeyUri(path)
}

private case class LocalSparkeyUri(basePath: String) extends SparkeyUri {
  override def getReader(): SparkeyReader = Sparkey.open(new File(basePath))
}

private case class GcsSparkeyUri(basePath: String) extends SparkeyUri {
  val localBasePath: String = sys.props("java.io.tmpdir") + "/" + hashPrefix(basePath)

  override def getReader(): SparkeyReader = {
    for (ext <- Seq("spi", "spl")) {
      val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())
      ScioUtil.fetchFromGCS(gcs, new URI(s"$basePath.$ext"), s"$localBasePath.$ext")
    }
    Sparkey.open(new File(s"$localBasePath.spi"))
  }

  private def hashPrefix(path: String): String =
    Hashing.sha1().hashString(path, Charsets.UTF_8).toString.substring(0, 8) + "-sparkey"
}

private[sparkey] class SparkeyWriter(val uri: SparkeyUri) {
  private lazy val localFile = uri match {
    case LocalSparkeyUri(_) => uri.toString()
    case gcsUri: GcsSparkeyUri => gcsUri.localBasePath
  }

  private lazy val delegate = Sparkey.createNew(new File(localFile), CompressionType.NONE, 512)

  def put(key: String, value: String): Unit = delegate.put(key, value)

  def close(): Unit = {
    delegate.flush()
    delegate.writeHash()
    delegate.close()
    uri match {
      case GcsSparkeyUri(path) => {
        val gcs = new GcsUtilFactory().create(PipelineOptionsFactory.create())
        // Copy .spi and .spl to GCS path
        for (ext <- Seq("spi", "spl")) {
          val writer = gcs.create(GcsPath.fromUri(s"$path.$ext"), "application/octet-stream")
          writer.write(ByteBuffer.wrap(Files.readAllBytes(Paths.get(s"$localFile.$ext"))))
          writer.close()
        }
      }
      case _ => ()
    }
  }
}
