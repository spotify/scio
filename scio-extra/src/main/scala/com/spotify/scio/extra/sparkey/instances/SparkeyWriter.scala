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

package com.spotify.scio.extra.sparkey.instances

import com.spotify.scio.extra.sparkey.SparkeyUri
import com.spotify.scio.util.RemoteFileUtil
import com.spotify.sparkey.{CompressionType, Sparkey}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}
import scala.util.control.NonFatal

private[sparkey] class SparkeyWriter(
  val uri: SparkeyUri,
  rfu: RemoteFileUtil,
  compressionType: CompressionType,
  compressionBlockSize: Int,
  maxMemoryUsage: Long = -1
) {
  private lazy val localFile =
    if (uri.isLocal) uri.basePath
    else {
      Files.createTempDirectory("sparkey-").resolve("data").toString
    }

  private lazy val delegate = {
    val file = new File(localFile)
    Files.createDirectories(file.getParentFile.toPath)
    Sparkey.createNew(file, compressionType, compressionBlockSize)
  }

  def put(key: String, value: String): Unit = delegate.put(key, value)

  def put(key: Array[Byte], value: Array[Byte]): Unit = delegate.put(key, value)

  def close(): Unit = {
    delegate.flush()
    if (maxMemoryUsage > 0) {
      delegate.setMaxMemory(maxMemoryUsage)
    }
    delegate.writeHash()
    delegate.close()

    if (!uri.isLocal) {
      // Copy .spi and .spl to the remote file system
      SparkeyUri.extensions.foreach { e =>
        val src = Paths.get(localFile + e)
        val dst = new URI(uri.basePath + e)
        val size = Files.size(src)
        rfu.upload(src, dst)
        SparkeyWriter.logger.info(s"Uploaded $src to $dst ($size bytes)")
      }
      // All uploads succeeded: the local copy is no longer needed. Readers always download
      // from the remote URI. If an upload fails, the exception propagates and the local files
      // are kept.
      deleteLocalFiles()
    }
  }

  private def deleteLocalFiles(): Unit = {
    val tempDir = Paths.get(localFile).getParent
    try {
      SparkeyUri.extensions.foreach(e => Files.deleteIfExists(Paths.get(localFile + e)))
      Files.deleteIfExists(tempDir)
      SparkeyWriter.logger.debug(s"Deleted local Sparkey temp dir $tempDir")
    } catch {
      case NonFatal(e) =>
        // The upload succeeded, so a failed cleanup must not fail the write.
        SparkeyWriter.logger.warn(s"Failed to delete local Sparkey temp dir $tempDir", e)
    }
  }
}

private[sparkey] object SparkeyWriter {
  private val logger = LoggerFactory.getLogger(classOf[SparkeyWriter])
}
