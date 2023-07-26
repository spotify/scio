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

import java.io.File
import java.net.URI
import java.nio.file.{Files, Paths}

private[sparkey] class SparkeyWriter(
  val uri: SparkeyUri,
  rfu: RemoteFileUtil,
  compressionType: CompressionType,
  compressionBlockSize: Int,
  maxMemoryUsage: Long = -1
) {
  private val localFile =
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
      // Copy .spi and .spl to GCS
      SparkeyUri.extensions.foreach { e =>
        val src = Paths.get(localFile + e)
        val dst = new URI(uri.basePath + e)
        rfu.upload(src, dst)
      }
    }
  }
}
