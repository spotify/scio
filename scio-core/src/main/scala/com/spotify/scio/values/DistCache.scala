/*
 * Copyright 2018 Spotify AB.
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

import com.spotify.scio.util.{RemoteFileUtil, ScioUtil}
import org.apache.beam.sdk.options.PipelineOptions

import scala.collection.JavaConverters._

/**
 * Encapsulate arbitrary data that can be distributed to all workers. Similar to Hadoop
 * distributed cache.
 */
sealed trait DistCache[F] extends Serializable {
  /** Extract the underlying data. */
  def apply(): F
}

private[scio] abstract class FileDistCache[F](options: PipelineOptions) extends DistCache[F] {

  override def apply(): F = data

  protected def init: F

  protected lazy val data: F = init

  private val rfu = RemoteFileUtil.create(options)
  private val isRemoteRunner = ScioUtil.isRemoteRunner(options.getRunner)

  protected def prepareFiles(uris: Seq[URI]): Seq[File] =
    rfu.download(uris.asJava).asScala.map(_.toFile)

  protected def verifyUri(uri: URI): Unit =
    if (isRemoteRunner) {
      require(ScioUtil.isRemoteUri(uri), s"Unsupported path $uri")
    }

}

private[scio] class MockDistCache[F](val value: F) extends DistCache[F] {
  override def apply(): F = value
}

private[scio] class MockDistCacheFunc[F](val value: () => F) extends DistCache[F] {
  override def apply(): F = value()
}

object MockDistCache {

  /**
   * Mock distCache by returning given value.
   *
   * @param value mock value, must be serializable.
   */
  def apply[F](value: F): DistCache[F] = new MockDistCache(value)

  /**
   * Mock distCache by returning result of given init function.
   *
   * @param initFn init function, must be serializable.
   */
  def apply[F](initFn: () => F): DistCache[F] = new MockDistCacheFunc[F](initFn)
}

private[scio] class DistCacheSingle[F](val uri: URI,
                                       val initFn: File => F,
                                       options: PipelineOptions)
  extends FileDistCache[F](options) {
  verifyUri(uri)
  override protected def init: F = initFn(prepareFiles(Seq(uri)).head)
}

private[scio] class DistCacheMulti[F](val uris: Seq[URI],
                                      val initFn: Seq[File] => F,
                                      options: PipelineOptions)
  extends FileDistCache[F](options) {
  uris.foreach(verifyUri)
  override protected def init: F = initFn(prepareFiles(uris))
}
