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
import java.nio.channels.{FileChannel, WritableByteChannel}
import java.nio.file.Paths

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.util.ScioUtil
import com.spotify.sparkey.{CompressionType, Sparkey, SparkeyReader}
import org.apache.beam.sdk.options.{GcsOptions, PipelineOptions}
import org.apache.beam.sdk.util.{GcsUtil, MimeTypes}
import org.apache.beam.sdk.util.gcsfs.GcsPath

import scala.util.Try

/**
 * Represents the base URI for a Sparkey index and log file, either on the local file
 * system or on GCS. For GCS, `basePath` should be in the form
 * 'gs://<bucket>/<path>/<sparkey-prefix>'. For local files, it should be in the form
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
    if (ScioUtil.isGcsUri(new URI(basePath))) {
      new GcsSparkeyUri(basePath, opts.as(classOf[GcsOptions]))
    } else {
      new LocalSparkeyUri(basePath)
    }
}

private class LocalSparkeyUri(val basePath: String) extends SparkeyUri {
  override def getReader: SparkeyReader = Sparkey.open(new File(basePath))
  override private[sparkey] def exists: Boolean =
    new File(basePath + ".spi").exists || new File(basePath + ".spl").exists
}

private class GcsSparkeyUri(val basePath: String, options: GcsOptions) extends SparkeyUri {

  def localBasePath: String = sys.props("java.io.tmpdir") + hashPrefix(basePath)
  private val json: String = new ObjectMapper().writeValueAsString(options)

  override def getReader: SparkeyReader = {
    for (ext <- Seq("spi", "spl")) {
      ScioUtil.fetchFromGCS(gcs, new URI(s"$basePath.$ext"), s"$localBasePath.$ext")
    }
    Sparkey.open(new File(s"$localBasePath.spi"))
  }

  override def exists: Boolean = {
    val index = GcsPath.fromUri(basePath + ".spi")
    val log = GcsPath.fromUri(basePath + ".spl")
    Try(gcs.fileSize(index)).isSuccess || Try(gcs.fileSize(log)).isSuccess
  }

  @transient private[sparkey] lazy val gcs: GcsUtil = new ObjectMapper()
    .readValue(json, classOf[PipelineOptions])
    .as(classOf[GcsOptions])
    .getGcsUtil

  private def hashPrefix(path: String): String =
    "sparkey-" + Hashing.sha1().hashString(path, Charsets.UTF_8).toString.substring(0, 8)

}

private[sparkey] class SparkeyWriter(val uri: SparkeyUri) {

  private val localFile = uri match {
    case u: LocalSparkeyUri => u.toString
    case u: GcsSparkeyUri => u.localBasePath
  }

  private lazy val delegate = Sparkey.createNew(new File(localFile), CompressionType.NONE, 512)

  def put(key: String, value: String): Unit = delegate.put(key, value)

  def put(key: Array[Byte], value: Array[Byte]): Unit = delegate.put(key, value)

  def close(): Unit = {
    delegate.flush()
    delegate.writeHash()
    delegate.close()
    uri match {
      case gcsUri: GcsSparkeyUri => {
        // Copy .spi and .spl to GCS path
        for (ext <- Seq("spi", "spl")) {
          val gcsFile = gcsUri.gcs.create(
            GcsPath.fromUri(s"${gcsUri.basePath}.$ext"),
            MimeTypes.BINARY)
          val localFileChannel = FileChannel.open(Paths.get(s"$localFile.$ext"))
          transferFile(localFileChannel, gcsFile)
        }
      }
      case _ => ()
    }
  }

  private def transferFile(srcChannel: FileChannel, destination: WritableByteChannel): Unit = {
    var position = 0L
    val srcFileSize = srcChannel.size()
    while (position < srcFileSize) {
      position += srcChannel.transferTo(position, srcFileSize - position, destination)
    }
    srcChannel.close()
    destination.close()
  }
}
