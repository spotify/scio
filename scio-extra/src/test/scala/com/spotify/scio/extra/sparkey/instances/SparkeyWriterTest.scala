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

  /**
   * Records uploads instead of writing to a remote file system. Fails the upload with the given
   * 1-based index, if set. Runs `afterUpload` after each successful upload.
   */
  private class FakeRemoteFileUtil(failOn: Option[Int] = None, afterUpload: Path => Unit = _ => ())
      extends RemoteFileUtil {
    val uploaded: mutable.Buffer[(Path, URI)] = mutable.Buffer.empty
    override def upload(src: Path, dst: URI): Unit = {
      uploaded += (src -> dst)
      if (failOn.contains(uploaded.size)) throw new IOException("simulated upload failure")
      afterUpload(src)
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

  private def tempDirOf(rfu: FakeRemoteFileUtil): Option[Path] =
    rfu.uploaded.headOption.map(_._1.getParent)

  private def cleanUp(rfu: FakeRemoteFileUtil): Unit =
    tempDirOf(rfu).foreach(d => FileUtils.deleteQuietly(d.toFile))

  "SparkeyWriter" should "delete local temp files after a successful remote upload" in {
    val rfu = new FakeRemoteFileUtil()
    try {
      write(rfu).close()

      rfu.uploaded.map(_._2.toString) should contain theSameElementsAs Seq(
        "gs://fake-bucket/sparkey-test/part-00000-of-00001.spi",
        "gs://fake-bucket/sparkey-test/part-00000-of-00001.spl"
      )
      all(rfu.uploaded.map(u => Files.exists(u._1))) shouldBe false
      Files.exists(tempDirOf(rfu).get) shouldBe false
    } finally cleanUp(rfu)
  }

  it should "keep local temp files when the first remote upload fails" in {
    val rfu = new FakeRemoteFileUtil(failOn = Some(1))
    try {
      an[IOException] should be thrownBy write(rfu).close()

      rfu.uploaded should have size 1
      SparkeyUri.extensions.foreach(e =>
        Files.exists(tempDirOf(rfu).get.resolve("data" + e)) shouldBe true
      )
    } finally cleanUp(rfu)
  }

  it should "keep local temp files when a later remote upload fails" in {
    val rfu = new FakeRemoteFileUtil(failOn = Some(2))
    try {
      an[IOException] should be thrownBy write(rfu).close()

      rfu.uploaded should have size 2
      SparkeyUri.extensions.foreach(e =>
        Files.exists(tempDirOf(rfu).get.resolve("data" + e)) shouldBe true
      )
    } finally cleanUp(rfu)
  }

  it should "not fail the write when the cleanup fails" in {
    // An extra file in the temp dir makes the delete of the dir fail.
    val rfu = new FakeRemoteFileUtil(afterUpload =
      src => Files.write(src.getParent.resolve("extra"), Array[Byte](1))
    )
    try {
      noException should be thrownBy write(rfu).close()

      rfu.uploaded should have size 2
      Files.exists(tempDirOf(rfu).get) shouldBe true
    } finally cleanUp(rfu)
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
