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

package com.spotify.scio.values

import java.io.{File, FileOutputStream}
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.cloud.dataflow.sdk.options.{GcsOptions, PipelineOptions}
import com.google.cloud.dataflow.sdk.util.gcsfs.GcsPath
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.util.ScioUtil
import org.slf4j.{Logger, LoggerFactory}

/** Encapsulate files on Google Cloud Storage that can be distributed to all workers. */
sealed trait DistCache[F] extends Serializable {
  /** Extract the underlying data. */
  def apply(): F
}

private[scio] object FileDistCache

private[scio] abstract class FileDistCache[F](options: GcsOptions) extends DistCache[F] {

  override def apply(): F = data

  protected def init: F

  protected lazy val data: F = init

  private val logger: Logger = LoggerFactory.getLogger(classOf[DistCache[_]])

  // Serialize options to avoid shipping it with closure
  private val json: String = new ObjectMapper().writeValueAsString(options)
  private def opts: GcsOptions = new ObjectMapper()
    .readValue(json, classOf[PipelineOptions])
    .as(classOf[GcsOptions])

  private def fetchFromGCS(uri: URI, prefix: String): File = {
    val path = prefix + uri.getPath.split("/").last
    val file = new File(path)

    // There can be multiple DoFns/Threads trying to fetch the same data/files on the same
    // worker. To prevent from situation where some workers see incomplete data, and keep the
    // solution simple - let's synchronize on FileDistCache companion object (which is a singleton).
    // There is a downside - more specifically we might have to wait a bit longer then in more
    // optimal solution, but, simplicity > performance.
    FileDistCache.synchronized {
      val gcsUtil = opts.getGcsUtil
      val src = gcsUtil.open(GcsPath.fromUri(uri))

      if (file.exists() && src.size() != file.length()) {
        // File exists but has different size than source file, most likely there was an issue
        // on previous thread, let's remove invalid file, and download it again.
        file.delete()
      }

      if (!file.exists()) {
        val fos: FileOutputStream = new FileOutputStream(path)
        val dst = fos.getChannel
        val src = gcsUtil.open(GcsPath.fromUri(uri))

        val size = dst.transferFrom(src, 0, src.size())
        logger.info(s"DistCache $uri fetched to $path, size: $size")

        dst.close()
        fos.close()
      } else {
        logger.info(s"DistCache $uri already fetched ")
      }

      file
    }
  }

  private def temporaryPrefix(uris: Seq[URI]): String = {
    val hash = Hashing.sha1().hashString(uris.map(_.toString).mkString("|"), Charsets.UTF_8)
    sys.props("java.io.tmpdir") + "/" + hash.toString.substring(0, 8) + "-"
  }

  protected def prepareFiles(uris: Seq[URI]): Seq[File] = uris.map { u =>
    if (ScioUtil.isGcsUri(u)) {
      fetchFromGCS(u, temporaryPrefix(uris))
    } else {
      new File(u.toString)
    }
  }

  protected def verifyUri(uri: URI): Unit = {
    if (ScioUtil.isLocalRunner(opts)) {
      require(ScioUtil.isLocalUri(uri) || ScioUtil.isGcsUri(uri), s"Unsupported path $uri")
    } else {
      require(ScioUtil.isGcsUri(uri), s"Unsupported path $uri")
    }
  }

}

private[scio] class MockDistCache[F](val value: F) extends DistCache[F] {
  override def apply(): F = value
}

private[scio] class DistCacheSingle[F](val uri: URI, val initFn: File => F, options: GcsOptions)
  extends FileDistCache[F](options) {
  verifyUri(uri)
  override protected def init: F = initFn(prepareFiles(Seq(uri)).head)
}

private[scio] class DistCacheMulti[F](val uris: Seq[URI],
                                      val initFn: Seq[File] => F,
                                      options: GcsOptions)
  extends FileDistCache[F](options) {
  uris.foreach(verifyUri)
  override protected def init: F = initFn(prepareFiles(uris))
}
