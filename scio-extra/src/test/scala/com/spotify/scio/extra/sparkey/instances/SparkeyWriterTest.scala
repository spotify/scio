/*
 * Copyright 2026 Spotify AB.
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
import com.spotify.sparkey.CompressionType
import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.net.URI
import java.nio.file.{Files, Path}
import scala.collection.mutable

class SparkeyWriterTest extends AnyFlatSpec with Matchers {

  /** Records uploads and copies files to a local target instead of a remote file system. */
  private class FakeRemoteFileUtil(target: Path, fail: Boolean) extends RemoteFileUtil {
    val uploaded: mutable.Buffer[(Path, URI)] = mutable.Buffer.empty
    override def upload(src: Path, dst: URI): Unit = {
      uploaded += (src -> dst)
      if (fail) throw new IOException("simulated upload failure")
      Files.copy(src, target.resolve(src.getFileName.toString + uploaded.size))
    }
  }

  private def write(rfu: RemoteFileUtil): SparkeyWriter = {
    val writer = new SparkeyWriter(
      SparkeyUri("gs://fake-bucket/sparkey-test/part-00000-of-00001"),
      rfu,
      CompressionType.NONE,
      0
    )
    (1 to 100).foreach(i => writer.put(s"k$i", s"v$i"))
    writer
  }

  "SparkeyWriter" should "delete local temp files after a successful remote upload" in {
    val target = Files.createTempDirectory("sparkey-writer-test-")
    try {
      val rfu = new FakeRemoteFileUtil(target, fail = false)
      write(rfu).close()

      rfu.uploaded.map(_._2.toString) should contain theSameElementsAs Seq(
        "gs://fake-bucket/sparkey-test/part-00000-of-00001.spi",
        "gs://fake-bucket/sparkey-test/part-00000-of-00001.spl"
      )
      val srcs = rfu.uploaded.map(_._1)
      all(srcs.map(Files.exists(_))) shouldBe false
      Files.exists(srcs.head.getParent) shouldBe false
    } finally FileUtils.deleteDirectory(target.toFile)
  }

  it should "keep local temp files when the remote upload fails" in {
    val target = Files.createTempDirectory("sparkey-writer-test-")
    try {
      val rfu = new FakeRemoteFileUtil(target, fail = true)
      an[IOException] should be thrownBy write(rfu).close()

      val src = rfu.uploaded.head._1
      Files.exists(src) shouldBe true
      FileUtils.deleteDirectory(src.getParent.toFile)
    } finally FileUtils.deleteDirectory(target.toFile)
  }

  it should "not delete the output of a local write" in {
    val dir = Files.createTempDirectory("sparkey-writer-local-")
    try {
      val base = dir.resolve("data").toString
      val writer = new SparkeyWriter(SparkeyUri(base), null, CompressionType.NONE, 0)
      writer.put("k", "v")
      writer.close()
      SparkeyUri.extensions.foreach(e => Files.exists(dir.resolve("data" + e)) shouldBe true)
    } finally FileUtils.deleteDirectory(dir.toFile)
  }
}
