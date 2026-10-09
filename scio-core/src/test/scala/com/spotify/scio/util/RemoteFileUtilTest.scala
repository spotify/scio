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

package com.spotify.scio.util

import com.spotify.scio.util.FakeRemoteFileSystemRegistrar.Entry
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.hash.Hashing
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.IOException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.Random

class RemoteFileUtilTest extends AnyFlatSpec with Matchers {
  private val Size = 1024 * 1024
  // Guards against the download loop spinning forever; a correct implementation finishes in ms.
  private val Timeout = 30.seconds

  private lazy val rfu = RemoteFileUtil.create(PipelineOptionsFactory.create())

  private def newUri(): URI =
    URI.create(s"${FakeRemoteFileSystemRegistrar.SCHEME}://bucket/${UUID.randomUUID()}/data.bin")

  private def bytes(n: Int): Array[Byte] = {
    val b = new Array[Byte](n)
    new Random(42).nextBytes(b)
    b
  }

  // Run on a separate (daemon) thread so that a spinning download fails the test instead of
  // hanging the build.
  private def withTimeout[T](f: => T): T =
    Await.result(Future(f)(ExecutionContext.global), Timeout)

  // Mirrors RemoteFileUtil's local layout: <java.io.tmpdir>/fd-<scheme>-<hash of prefix>/<name>
  private def localPath(uri: URI): Path = {
    val s = uri.toString
    val idx = s.lastIndexOf('/')
    val hash = Hashing
      .murmur3_128()
      .hashString(s.substring(0, idx), StandardCharsets.UTF_8)
      .toString
      .substring(0, 8)
    Paths.get(
      System.getProperty("java.io.tmpdir"),
      s"fd-${uri.getScheme}-$hash",
      s.substring(idx + 1)
    )
  }

  private def rootCause(t: Throwable): Throwable =
    Iterator.iterate(t)(_.getCause).takeWhile(_ != null).toSeq.last

  "RemoteFileUtil" should "download a remote file" in {
    val uri = newUri()
    val content = bytes(Size)
    FakeRemoteFileSystemRegistrar.put(uri.toString, new Entry(Size.toLong, content, -1))
    val openBefore = FakeRemoteFileSystemRegistrar.openChannels()

    val path = withTimeout(rfu.download(uri))
    path shouldBe localPath(uri)
    Files.readAllBytes(path) shouldBe content
    FakeRemoteFileSystemRegistrar.openChannels() shouldBe openBefore
    rfu.delete(uri)
  }

  it should "download an empty remote file" in {
    val uri = newUri()
    FakeRemoteFileSystemRegistrar.put(uri.toString, new Entry(0L, Array.emptyByteArray, -1))
    val openBefore = FakeRemoteFileSystemRegistrar.openChannels()

    val path = withTimeout(rfu.download(uri))
    Files.size(path) shouldBe 0L
    FakeRemoteFileSystemRegistrar.openChannels() shouldBe openBefore
    rfu.delete(uri)
  }

  it should "fail instead of hanging when the source ends before its declared size" in {
    val uri = newUri()
    FakeRemoteFileSystemRegistrar.put(uri.toString, new Entry(Size.toLong, bytes(Size / 2), -1))
    val openBefore = FakeRemoteFileSystemRegistrar.openChannels()

    val e = the[RuntimeException] thrownBy withTimeout(rfu.download(uri))
    rootCause(e) shouldBe an[IOException]
    rootCause(e).getMessage should include(s"${Size / 2} of $Size bytes")
    Files.exists(localPath(uri)) shouldBe false
    FakeRemoteFileSystemRegistrar.openChannels() shouldBe openBefore
  }

  it should "fail instead of hanging in a batch download" in {
    val good = newUri()
    val bad = newUri()
    FakeRemoteFileSystemRegistrar.put(good.toString, new Entry(Size.toLong, bytes(Size), -1))
    FakeRemoteFileSystemRegistrar.put(bad.toString, new Entry(Size.toLong, bytes(Size / 2), -1))

    val e = the[RuntimeException] thrownBy withTimeout(rfu.download(List(good, bad).asJava))
    rootCause(e) shouldBe an[IOException]
    rfu.delete(good)
  }

  it should "delete the partial local file and allow a retry when the source fails" in {
    val uri = newUri()
    val content = bytes(Size)
    FakeRemoteFileSystemRegistrar.put(uri.toString, new Entry(Size.toLong, content, Size / 2L))
    val openBefore = FakeRemoteFileSystemRegistrar.openChannels()

    val e = the[RuntimeException] thrownBy withTimeout(rfu.download(uri))
    rootCause(e).getMessage should include("Simulated read failure")
    Files.exists(localPath(uri)) shouldBe false
    FakeRemoteFileSystemRegistrar.openChannels() shouldBe openBefore

    // The source recovers: the next download must succeed and produce the full file.
    FakeRemoteFileSystemRegistrar.put(uri.toString, new Entry(Size.toLong, content, -1))
    val path = withTimeout(rfu.download(uri))
    Files.readAllBytes(path) shouldBe content
    rfu.delete(uri)
  }
}
