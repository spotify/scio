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

import java.io.File
import java.net.URI

import com.fasterxml.jackson.databind.ObjectMapper
import com.google.common.base.Charsets
import com.google.common.hash.Hashing
import com.spotify.scio.util.ScioUtil
import org.apache.beam.runners.direct.DirectRunner
import org.apache.beam.sdk.options.{GcsOptions, PipelineOptions}

/**
 * Encapsulate arbitrary data that can be distributed to all workers. Similar to Hadoop
 * distributed cache.
 */
sealed trait DistCache[F] extends Serializable {
  /** Extract the underlying data. */
  def apply(): F
}

private[scio] abstract class FileDistCache[F](options: GcsOptions) extends DistCache[F] {

  override def apply(): F = data

  protected def init: F

  protected lazy val data: F = init

  // Serialize options to avoid shipping it with closure
  private val json: String = new ObjectMapper().writeValueAsString(options)
  private def opts: GcsOptions = new ObjectMapper()
    .readValue(json, classOf[PipelineOptions])
    .as(classOf[GcsOptions])

  private def temporaryPrefix(uris: Seq[URI]): String = {
    val hash = Hashing.sha1().hashString(uris.map(_.toString).mkString("|"), Charsets.UTF_8)
    sys.props("java.io.tmpdir") + "/" + hash.toString.substring(0, 8) + "-"
  }

  protected def prepareFiles(uris: Seq[URI]): Seq[File] = {
    val prefix = temporaryPrefix(uris)
    uris.map { u =>
      if (ScioUtil.isGcsUri(u)) {
        val dest = prefix + u.getPath.split("/").last
        ScioUtil.fetchFromGCS(opts.getGcsUtil, u, dest)
      } else {
        new File(u.toString)
      }
    }
  }

  protected def verifyUri(uri: URI): Unit = {
    if (classOf[DirectRunner] isAssignableFrom opts.getRunner) {
      require(ScioUtil.isLocalUri(uri) || ScioUtil.isGcsUri(uri), s"Unsupported path $uri")
    } else {
      require(ScioUtil.isGcsUri(uri), s"Unsupported path $uri")
    }
  }

}

private[scio] class MockDistCache[F](val value: F) extends DistCache[F] {
  override def apply(): F = value
}

object MockDistCache {
  def apply[F](value: F): DistCache[F] = new MockDistCache(value)
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
