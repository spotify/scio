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

package com.spotify.scio.io

import java.io.{File, InputStream}
import java.net.URI
import java.nio.file.Path

import org.apache.avro.file.{SeekableFileInput, SeekableInput}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, PathFilter}
import org.apache.hadoop.io.compress.CompressionCodecFactory

object HdfsFileStorage {
  def apply(path: String): FileStorage = new HdfsFileStorage(path)
}


private class HdfsFileStorage(protected[io] val path: String) extends FileStorage {

  private val pathFilter = new PathFilter {
    override def accept(path: org.apache.hadoop.fs.Path): Boolean =
      !path.getName.startsWith("_") && !path.getName.startsWith(".")
  }

  override protected def listFiles: Seq[Path] = {
    val conf = new Configuration()
    val fs = FileSystem.get(new URI(path), conf)

    fs
      .listStatus(new org.apache.hadoop.fs.Path(path), pathFilter)
      .map(status => new File(status.getPath.toString).toPath).toSeq
  }

  override protected def getObjectInputStream(path: Path): InputStream = {
    val hPath = new org.apache.hadoop.fs.Path(path.toString)
    val conf = new Configuration()
    val factory = new CompressionCodecFactory(conf)
    val fs = FileSystem.get(path.toUri, conf)
    val codec = factory.getCodec(hPath)
    if (codec != null) {
      codec.createInputStream(fs.open(hPath))
    } else {
      fs.open(hPath)
    }
  }

  override protected def getAvroSeekableInput(path: Path): SeekableInput = {
    val hPath = new org.apache.hadoop.fs.Path(path.toString)
    new SeekableFileInput(new File(hPath.toUri))
  }

}
